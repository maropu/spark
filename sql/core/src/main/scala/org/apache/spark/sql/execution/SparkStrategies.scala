/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution
import org.apache.spark.sql.execution.columnar.{InMemoryRelation, InMemoryTableScanExec}
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.exchange.ShuffleExchange
import org.apache.spark.sql.execution.joins.{BuildLeft, BuildRight}
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.StreamingQuery

/**
 * Converts a logical plan into zero or more SparkPlans.  This API is exposed for experimenting
 * with the query planner and is not designed to be stable across spark releases.  Developers
 * writing libraries should instead consider using the stable APIs provided in
 * [[org.apache.spark.sql.sources]]
 */
abstract class SparkStrategy extends GenericStrategy[SparkPlan] {

  override protected def planLater(plan: LogicalPlan): SparkPlan = PlanLater(plan)
}

case class PlanLater(plan: LogicalPlan) extends LeafExecNode {

  override def output: Seq[Attribute] = plan.output

  protected override def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException()
  }
}

abstract class SparkStrategies extends QueryPlanner[SparkPlan] {
  self: SparkPlanner =>

  /**
   * Plans special cases of limit operators.
   */
  object SpecialLimits extends Strategy {
    override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case ReturnAnswer(rootPlan) => rootPlan match {
        case Limit(IntegerLiteral(limit), Sort(order, true, child)) =>
          TakeOrderedAndProjectExec(limit, order, child.output, planLater(child)) :: Nil
        case Limit(IntegerLiteral(limit), Project(projectList, Sort(order, true, child))) =>
          TakeOrderedAndProjectExec(limit, order, projectList, planLater(child)) :: Nil
        case Limit(IntegerLiteral(limit), child) =>
          // With whole stage codegen, Spark releases resources only when all the output data of the
          // query plan are consumed. It's possible that `CollectLimitExec` only consumes a little
          // data from child plan and finishes the query without releasing resources. Here we wrap
          // the child plan with `LocalLimitExec`, to stop the processing of whole stage codegen and
          // trigger the resource releasing work, after we consume `limit` rows.
          CollectLimitExec(limit, LocalLimitExec(limit, planLater(child))) :: Nil
        case other => planLater(other) :: Nil
      }
      case Limit(IntegerLiteral(limit), Sort(order, true, child)) =>
        TakeOrderedAndProjectExec(limit, order, child.output, planLater(child)) :: Nil
      case Limit(IntegerLiteral(limit), Project(projectList, Sort(order, true, child))) =>
        TakeOrderedAndProjectExec(limit, order, projectList, planLater(child)) :: Nil
      case _ => Nil
    }
  }

  /**
   * Select the proper physical plan for join based on joining keys and size of logical plan.
   *
   * At first, uses the [[ExtractEquiJoinKeys]] pattern to find joins where at least some of the
   * predicates can be evaluated by matching join keys. If found,  Join implementations are chosen
   * with the following precedence:
   *
   * - Broadcast: if one side of the join has an estimated physical size that is smaller than the
   *     user-configurable [[SQLConf.AUTO_BROADCASTJOIN_THRESHOLD]] threshold
   *     or if that side has an explicit broadcast hint (e.g. the user applied the
   *     [[org.apache.spark.sql.functions.broadcast()]] function to a DataFrame), then that side
   *     of the join will be broadcasted and the other side will be streamed, with no shuffling
   *     performed. If both sides of the join are eligible to be broadcasted then the
   * - Shuffle hash join: if the average size of a single partition is small enough to build a hash
   *     table.
   * - Sort merge: if the matching join keys are sortable.
   *
   * If there is no joining keys, Join implementations are chosen with the following precedence:
   * - BroadcastNestedLoopJoin: if one side of the join could be broadcasted
   * - CartesianProduct: for Inner join
   * - BroadcastNestedLoopJoin
   */
  object JoinSelection extends Strategy with PredicateHelper {

    /**
     * Matches a plan whose output should be small enough to be used in broadcast join.
     */
    private def canBroadcast(plan: LogicalPlan): Boolean = {
      plan.stats.hints.broadcast ||
        (plan.stats.sizeInBytes >= 0 &&
          plan.stats.sizeInBytes <= conf.autoBroadcastJoinThreshold)
    }

    /**
     * Matches a plan whose single partition should be small enough to build a hash table.
     *
     * Note: this assume that the number of partition is fixed, requires additional work if it's
     * dynamic.
     */
    private def canBuildLocalHashMap(plan: LogicalPlan): Boolean = {
      plan.stats.sizeInBytes < conf.autoBroadcastJoinThreshold * conf.numShufflePartitions
    }

    /**
     * Returns whether plan a is much smaller (3X) than plan b.
     *
     * The cost to build hash map is higher than sorting, we should only build hash map on a table
     * that is much smaller than other one. Since we does not have the statistic for number of rows,
     * use the size of bytes here as estimation.
     */
    private def muchSmaller(a: LogicalPlan, b: LogicalPlan): Boolean = {
      a.stats.sizeInBytes * 3 <= b.stats.sizeInBytes
    }

    private def canBuildRight(joinType: JoinType): Boolean = joinType match {
      case _: InnerLike | LeftOuter | LeftSemi | LeftAnti => true
      case j: ExistenceJoin => true
      case _ => false
    }

    private def canBuildLeft(joinType: JoinType): Boolean = joinType match {
      case _: InnerLike | RightOuter => true
      case _ => false
    }

    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {

      // --- BroadcastHashJoin --------------------------------------------------------------------

      case ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, condition, left, right)
        if canBuildRight(joinType) && canBroadcast(right) =>
        Seq(joins.BroadcastHashJoinExec(
          leftKeys, rightKeys, joinType, BuildRight, condition, planLater(left), planLater(right)))

      case ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, condition, left, right)
        if canBuildLeft(joinType) && canBroadcast(left) =>
        Seq(joins.BroadcastHashJoinExec(
          leftKeys, rightKeys, joinType, BuildLeft, condition, planLater(left), planLater(right)))

      // --- ShuffledHashJoin ---------------------------------------------------------------------

      case ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, condition, left, right)
         if !conf.preferSortMergeJoin && canBuildRight(joinType) && canBuildLocalHashMap(right)
           && muchSmaller(right, left) ||
           !RowOrdering.isOrderable(leftKeys) =>
        Seq(joins.ShuffledHashJoinExec(
          leftKeys, rightKeys, joinType, BuildRight, condition, planLater(left), planLater(right)))

      case ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, condition, left, right)
         if !conf.preferSortMergeJoin && canBuildLeft(joinType) && canBuildLocalHashMap(left)
           && muchSmaller(left, right) ||
           !RowOrdering.isOrderable(leftKeys) =>
        Seq(joins.ShuffledHashJoinExec(
          leftKeys, rightKeys, joinType, BuildLeft, condition, planLater(left), planLater(right)))

      // --- SortMergeJoin ------------------------------------------------------------

      case ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, condition, left, right)
        if RowOrdering.isOrderable(leftKeys) =>
        joins.SortMergeJoinExec(
          leftKeys, rightKeys, joinType, condition, planLater(left), planLater(right)) :: Nil

      // --- Without joining keys ------------------------------------------------------------

      // Pick BroadcastNestedLoopJoin if one side could be broadcasted
      case j @ logical.Join(left, right, joinType, condition)
          if canBuildRight(joinType) && canBroadcast(right) =>
        joins.BroadcastNestedLoopJoinExec(
          planLater(left), planLater(right), BuildRight, joinType, condition) :: Nil
      case j @ logical.Join(left, right, joinType, condition)
          if canBuildLeft(joinType) && canBroadcast(left) =>
        joins.BroadcastNestedLoopJoinExec(
          planLater(left), planLater(right), BuildLeft, joinType, condition) :: Nil

      // Pick CartesianProduct for InnerJoin
      case logical.Join(left, right, _: InnerLike, condition) =>
        joins.CartesianProductExec(planLater(left), planLater(right), condition) :: Nil

      case logical.Join(left, right, joinType, condition) =>
        val buildSide =
          if (right.stats.sizeInBytes <= left.stats.sizeInBytes) {
            BuildRight
          } else {
            BuildLeft
          }
        // This join could be very slow or OOM
        joins.BroadcastNestedLoopJoinExec(
          planLater(left), planLater(right), buildSide, joinType, condition) :: Nil

      // --- Cases where this strategy does not apply ---------------------------------------------

      case _ => Nil
    }
  }

  /**
   * Used to plan streaming aggregation queries that are computed incrementally as part of a
   * [[StreamingQuery]]. Currently this rule is injected into the planner
   * on-demand, only when planning in a [[org.apache.spark.sql.execution.streaming.StreamExecution]]
   */
  object StatefulAggregationStrategy extends Strategy {
    override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case _ if !plan.isStreaming => Nil

      case EventTimeWatermark(columnName, delay, child) =>
        EventTimeWatermarkExec(columnName, delay, planLater(child)) :: Nil

      case PhysicalAggregation(
        namedGroupingExpressions, aggregateExpressions, rewrittenResultExpressions, child) =>

        aggregate.AggUtils.planStreamingAggregation(
          namedGroupingExpressions,
          aggregateExpressions,
          rewrittenResultExpressions,
          planLater(child))

      case _ => Nil
    }
  }

  /**
   * Used to plan the streaming deduplicate operator.
   */
  object StreamingDeduplicationStrategy extends Strategy {
    override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case Deduplicate(keys, child) if child.isStreaming =>
        StreamingDeduplicateExec(keys, planLater(child)) :: Nil

      case _ => Nil
    }
  }

  /**
   * Used to plan the aggregate operator for expressions based on the AggregateFunction2 interface.
   */
  object Aggregation extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case PhysicalAggregation(
          groupingExpressions, aggregateExpressions, resultExpressions, child) =>

        val (functionsWithDistinct, functionsWithoutDistinct) =
          aggregateExpressions.partition(_.isDistinct)
        if (functionsWithDistinct.map(_.aggregateFunction.children).distinct.length > 1) {
          // This is a sanity check. We should not reach here when we have multiple distinct
          // column sets. Our MultipleDistinctRewriter should take care this case.
          sys.error("You hit a query analyzer bug. Please report your query to " +
              "Spark user mailing list.")
        }

        val aggregateOperator =
          if (functionsWithDistinct.isEmpty) {
            aggregate.AggUtils.planAggregateWithoutDistinct(
              groupingExpressions,
              aggregateExpressions,
              resultExpressions,
              planLater(child))
          } else {
            aggregate.AggUtils.planAggregateWithOneDistinct(
              groupingExpressions,
              functionsWithDistinct,
              functionsWithoutDistinct,
              resultExpressions,
              planLater(child))
          }

        aggregateOperator

      case _ => Nil
    }
  }

  protected lazy val singleRowRdd = sparkContext.parallelize(Seq(InternalRow()), 1)

  object InMemoryScans extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case PhysicalOperation(projectList, filters, mem: InMemoryRelation) =>
        pruneFilterProject(
          projectList,
          filters,
          identity[Seq[Expression]], // All filters still need to be evaluated.
          InMemoryTableScanExec(_, filters, mem)) :: Nil
      case _ => Nil
    }
  }

  /**
   * This strategy is just for explaining `Dataset/DataFrame` created by `spark.readStream`.
   * It won't affect the execution, because `StreamingRelation` will be replaced with
   * `StreamingExecutionRelation` in `StreamingQueryManager` and `StreamingExecutionRelation` will
   * be replaced with the real relation using the `Source` in `StreamExecution`.
   */
  object StreamingRelationStrategy extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case s: StreamingRelation =>
        StreamingRelationExec(s.sourceName, s.output) :: Nil
      case s: StreamingExecutionRelation =>
        StreamingRelationExec(s.toString, s.output) :: Nil
      case _ => Nil
    }
  }

  /**
   * Strategy to convert [[FlatMapGroupsWithState]] logical operator to physical operator
   * in streaming plans. Conversion for batch plans is handled by [[BasicOperators]].
   */
  object FlatMapGroupsWithStateStrategy extends Strategy {
    override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case FlatMapGroupsWithState(
        func, keyDeser, valueDeser, groupAttr, dataAttr, outputAttr, stateEnc, outputMode, _,
        timeout, child) =>
        val execPlan = FlatMapGroupsWithStateExec(
          func, keyDeser, valueDeser, groupAttr, dataAttr, outputAttr, None, stateEnc, outputMode,
          timeout, batchTimestampMs = None, eventTimeWatermark = None, planLater(child))
        execPlan :: Nil
      case _ =>
        Nil
    }
  }

  // Can we automate these 'pass through' operations?
  object BasicOperators extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = {

      val replaceRules = new LogicalPlanVisitor[SparkPlan] {
        override def visitUnion(p: Union): SparkPlan = p match {
          case logical.Union(unionChildren) =>
            execution.UnionExec(unionChildren.map(visit))
        }
        override def visitGlobalLimit(p: GlobalLimit): SparkPlan = p match {
          case logical.GlobalLimit(IntegerLiteral(limit), child) =>
            execution.GlobalLimitExec(limit, visit(child))
        }
        override def visitAggregate(p: Aggregate): SparkPlan = {
          planLater(p)
        }
        override def visitExcept(p: Except): SparkPlan = {
          planLater(p)
        }
        override def visitDistinct(p: Distinct): SparkPlan = {
          planLater(p)
        }
        override def visitSample(p: Sample): SparkPlan = p match {
          case logical.Sample(lb, ub, withReplacement, seed, child) =>
            execution.SampleExec(lb, ub, withReplacement, seed, visit(child))
        }
        override def visitRepartitionByExpr(p: RepartitionByExpression): SparkPlan = p match {
          case logical.RepartitionByExpression(expressions, child, numPartitions) =>
            exchange.ShuffleExchange(HashPartitioning(
              expressions, numPartitions), visit(child))
        }
        override def visitFilter(p: Filter): SparkPlan = p match {
          case logical.Filter(condition, child) =>
            execution.FilterExec(condition, visit(child))
        }
        override def visitWindow(p: Window): SparkPlan = p match {
          case logical.Window(windowExprs, partitionSpec, orderSpec, child) =>
            execution.window.WindowExec(windowExprs, partitionSpec, orderSpec, visit(child))
        }
        override def visitGenerate(p: Generate): SparkPlan = p match {
          case g @ logical.Generate(generator, join, outer, _, _, child) =>
            execution.GenerateExec(
              generator, join = join, outer = outer, g.qualifiedGeneratorOutput,
              visit(child))
        }
        override def visitIntersect(p: Intersect): SparkPlan = p match {
          case logical.Intersect(left, right) =>
            throw new IllegalStateException(
              "logical intersect operator should have been replaced by semi-join in the optimizer")
        }
        override def visitScriptTransform(p: ScriptTransformation): SparkPlan = {
          throw new IllegalStateException()
        }
        override def visitRepartition(p: Repartition): SparkPlan = p match {
          case logical.Repartition(numPartitions, shuffle, child) =>
            if (shuffle) {
              ShuffleExchange(RoundRobinPartitioning(numPartitions), visit(child))
            } else {
              execution.CoalesceExec(numPartitions, visit(child))
            }
        }
        override def visitJoin(p: Join): SparkPlan = {
          planLater(p)
        }
        override def visitLocalLimit(p: LocalLimit): SparkPlan = p match {
          case logical.LocalLimit(IntegerLiteral(limit), child) =>
            execution.LocalLimitExec(limit, visit(child))
        }
        override def visitProject(p: Project): SparkPlan = p match {
          case logical.Project(projectList, child) =>
            execution.ProjectExec(projectList, visit(child))
        }
        override def visitExpand(p: Expand): SparkPlan = p match {
          case e @ logical.Expand(_, _, child) =>
            execution.ExpandExec(e.projections, e.output, visit(child))
        }
        override def visitPivot(p: Pivot): SparkPlan = {
          throw new IllegalStateException()
        }
        override def visitHint(p: ResolvedHint): SparkPlan = p match {
          case h: ResolvedHint => visit(h.child)
        }

        // For other plans
        override def default(p: LogicalPlan): SparkPlan = p match {
          case r: RunnableCommand => ExecutedCommandExec(r, r.children.map(visit))

          case MemoryPlan(sink, output) =>
            val encoder = RowEncoder(sink.schema)
            LocalTableScanExec(output, sink.allData.map(r => encoder.toRow(r).copy()))

          case logical.DeserializeToObject(deserializer, objAttr, child) =>
            execution.DeserializeToObjectExec(deserializer, objAttr, visit(child))
          case logical.SerializeFromObject(serializer, child) =>
            execution.SerializeFromObjectExec(serializer, visit(child))
          case logical.MapPartitions(f, objAttr, child) =>
            execution.MapPartitionsExec(f, objAttr, visit(child))
          case logical.MapPartitionsInR(f, p, b, is, os, objAttr, child) =>
            execution.MapPartitionsExec(
              execution.r.MapPartitionsRWrapper(f, p, b, is, os), objAttr, visit(child))
          case logical.FlatMapGroupsInR(
              f, p, b, is, os, key, value, grouping, data, objAttr, child) =>
            execution.FlatMapGroupsInRExec(f, p, b, is, os, key, value, grouping,
              data, objAttr, visit(child))
          case logical.MapElements(f, _, _, objAttr, child) =>
            execution.MapElementsExec(f, objAttr, visit(child))
          case logical.AppendColumns(f, _, _, in, out, child) =>
            execution.AppendColumnsExec(f, in, out, visit(child))
          case logical.AppendColumnsWithObject(f, childSer, newSer, child) =>
            execution.AppendColumnsWithObjectExec(f, childSer, newSer, visit(child))
          case logical.MapGroups(f, key, value, grouping, data, objAttr, child) =>
            execution.MapGroupsExec(f, key, value, grouping, data, objAttr, visit(child))
          case logical.FlatMapGroupsWithState(
          f, key, value, grouping, data, output, _, _, _, timeout, child) =>
            execution.MapGroupsExec(
              f, key, value, grouping, data, output, timeout, visit(child))
          case logical.CoGroup(
              f, key, lObj, rObj, lGroup, rGroup, lAttr, rAttr, oAttr, left, right) =>
            execution.CoGroupExec(
              f, key, lObj, rObj, lGroup, rGroup, lAttr, rAttr, oAttr,
              visit(left), visit(right))

          case logical.Sort(sortExprs, global, child) =>
            execution.SortExec(sortExprs, global, visit(child))
          case f: logical.TypedFilter =>
            execution.FilterExec(f.typedCondition(f.deserializer), visit(f.child))
          case logical.LocalRelation(output, data, _) =>
            LocalTableScanExec(output, data)
          case _: logical.OneRowRelation =>
            execution.RDDScanExec(Nil, singleRowRdd, "OneRowRelation")
          case r: logical.Range =>
            execution.RangeExec(r)
          case ExternalRDD(outputObjAttr, rdd) => ExternalRDDScanExec(outputObjAttr, rdd)
          case r: LogicalRDD =>
            RDDScanExec(r.output, r.rdd, "ExistingRDD", r.outputPartitioning, r.outputOrdering)
          case other =>
            planLater(other)
        }
      }

      replaceRules.visit(plan) :: Nil
    }
  }
}
