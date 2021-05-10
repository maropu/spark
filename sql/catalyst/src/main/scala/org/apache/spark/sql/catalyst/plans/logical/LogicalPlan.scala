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

package org.apache.spark.sql.catalyst.plans.logical

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.internal.Logging
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.statsEstimation.LogicalPlanStats
import org.apache.spark.sql.catalyst.trees.{BinaryLike, LeafLike, UnaryLike}
import org.apache.spark.sql.types.StructType


abstract class LogicalPlan
  extends QueryPlan[LogicalPlan]
  with AnalysisHelper
  with LogicalPlanStats
  with QueryPlanConstraints
  with Logging {

  /**
   * Metadata fields that can be projected from this node.
   * Should be overridden if the plan does not propagate its children's output.
   */
  def metadataOutput: Seq[Attribute] = children.flatMap(_.metadataOutput)

  /** Returns true if this subtree has data from a streaming data source. */
  def isStreaming: Boolean = children.exists(_.isStreaming)

  override def verboseStringWithSuffix(maxFields: Int): String = {
    super.verboseString(maxFields) + statsCache.map(", " + _.toString).getOrElse("")
  }

  /**
   * Returns the maximum number of rows that this plan may compute.
   *
   * Any operator that a Limit can be pushed passed should override this function (e.g., Union).
   * Any operator that can push through a Limit should override this function (e.g., Project).
   */
  def maxRows: Option[Long] = None

  /**
   * Returns the maximum number of rows this plan may compute on each partition.
   */
  def maxRowsPerPartition: Option[Long] = maxRows

  /**
   * Returns true if this expression and all its children have been resolved to a specific schema
   * and false if it still contains any unresolved placeholders. Implementations of LogicalPlan
   * can override this (e.g.
   * [[org.apache.spark.sql.catalyst.analysis.UnresolvedRelation UnresolvedRelation]]
   * should return `false`).
   */
  lazy val resolved: Boolean = expressions.forall(_.resolved) && childrenResolved

  override protected def statePrefix = if (!resolved) "'" else super.statePrefix

  /**
   * Returns true if all its children of this query plan have been resolved.
   */
  def childrenResolved: Boolean = children.forall(_.resolved)

  /**
   * Resolves a given schema to concrete [[Attribute]] references in this query plan. This function
   * should only be called on analyzed plans since it will throw [[AnalysisException]] for
   * unresolved [[Attribute]]s.
   */
  def resolve(schema: StructType, resolver: Resolver): Seq[Attribute] = {
    schema.map { field =>
      resolve(field.name :: Nil, resolver).map {
        case a: AttributeReference => a
        case _ => sys.error(s"can not handle nested schema yet...  plan $this")
      }.getOrElse {
        throw new AnalysisException(
          s"Unable to resolve ${field.name} given [${output.map(_.name).mkString(", ")}]")
      }
    }
  }

  private[this] lazy val childAttributes = AttributeSeq(children.flatMap(_.output))

  private[this] lazy val childMetadataAttributes = AttributeSeq(children.flatMap(_.metadataOutput))

  private[this] lazy val outputAttributes = AttributeSeq(output)

  private[this] lazy val outputMetadataAttributes = AttributeSeq(metadataOutput)

  /**
   * Optionally resolves the given strings to a [[NamedExpression]] using the input from all child
   * nodes of this LogicalPlan. The attribute is expressed as
   * string in the following form: `[scope].AttributeName.[nested].[fields]...`.
   */
  def resolveChildren(
      nameParts: Seq[String],
      resolver: Resolver): Option[NamedExpression] =
    childAttributes.resolve(nameParts, resolver)
      .orElse(childMetadataAttributes.resolve(nameParts, resolver))

  /**
   * Optionally resolves the given strings to a [[NamedExpression]] based on the output of this
   * LogicalPlan. The attribute is expressed as string in the following form:
   * `[scope].AttributeName.[nested].[fields]...`.
   */
  def resolve(
      nameParts: Seq[String],
      resolver: Resolver): Option[NamedExpression] =
    outputAttributes.resolve(nameParts, resolver)
      .orElse(outputMetadataAttributes.resolve(nameParts, resolver))

  /**
   * Given an attribute name, split it to name parts by dot, but
   * don't split the name parts quoted by backticks, for example,
   * `ab.cd`.`efg` should be split into two parts "ab.cd" and "efg".
   */
  def resolveQuoted(
      name: String,
      resolver: Resolver): Option[NamedExpression] = {
    resolve(UnresolvedAttribute.parseAttributeName(name), resolver)
  }

  /**
   * Refreshes (or invalidates) any metadata/data cached in the plan recursively.
   */
  def refresh(): Unit = children.foreach(_.refresh())

  /**
   * Returns the output ordering that this plan generates.
   */
  def outputOrdering: Seq[SortOrder] = Nil

  /**
   * Returns true iff `other`'s output is semantically the same, i.e.:
   *  - it contains the same number of `Attribute`s;
   *  - references are the same;
   *  - the order is equal too.
   */
  def sameOutput(other: LogicalPlan): Boolean = {
    val thisOutput = this.output
    val otherOutput = other.output
    thisOutput.length == otherOutput.length && thisOutput.zip(otherOutput).forall {
      case (a1, a2) => a1.semanticEquals(a2)
    }
  }

  override private[sql] def processPlan(append: String => Unit): Unit = {
    try {
      val subqueries = ArrayBuffer.empty[(LogicalPlan, Expression, LogicalPlan)]
      var currentOperatorID = 0
      currentOperatorID = processPlanSkippingSubqueries(append, currentOperatorID)
      getSubqueries(subqueries)
      var i = 0

      for (sub <- subqueries) {
        if (i == 0) {
          append("\n===== Subqueries =====\n\n")
        }
        i = i + 1
        append(s"Subquery:$i Hosting operator id = " +
          s"${sub._1.getOpId()} Hosting Expression = ${sub._2}\n")

        // // For each subquery expression in the parent plan, process its child plan to compute
        // // the explain output. In case of subquery reuse, we don't print subquery plan more
        // // than once. So we skip [[ReusedSubqueryExec]] here.
        // if (!sub._3.isInstanceOf[ReusedSubqueryExec]) {
        //   currentOperatorID = processPlanSkippingSubqueries(
        //     sub._3.child,
        //     append,
        //     currentOperatorID)
        // }
        append("\n")
      }
    } finally {
      removeTags()
    }
  }

  override protected def generateOperatorIDs(
      startOperatorID: Int,
      operatorIDs: ArrayBuffer[(Int, LogicalPlan)]): Int = {
    var currentOperationID = startOperatorID
    // // Skip the subqueries as they are not printed as part of main query block.
    // if (plan.isInstanceOf[BaseSubqueryExec]) {
    //   return currentOperationID
    // }
    foreachUp {
      // case p: WholeStageCodegenExec =>
      // case p: InputAdapter =>
      case other =>

        def setOpId(): Unit = if (other.getTagValue(QueryPlan.OP_ID_TAG).isEmpty) {
          currentOperationID += 1
          other.setTagValue(QueryPlan.OP_ID_TAG, currentOperationID)
          operatorIDs += ((currentOperationID, other))
        }

        other match {
          // case p: AdaptiveSparkPlanExec =>
          //   currentOperationID =
          //     generateOperatorIDs(p.executedPlan, currentOperationID, operatorIDs)
          //   setOpId()
          // case p: QueryStageExec =>
          //   currentOperationID = generateOperatorIDs(p.plan, currentOperationID, operatorIDs)
          //   setOpId()
          case _ =>
            setOpId()
            other.innerChildren.map(_.asInstanceOf[LogicalPlan]).foldLeft(currentOperationID) {
              (curId, plan) => plan.generateOperatorIDs(curId, operatorIDs)
            }
        }
    }
    currentOperationID
  }

  override private[sql] def generateWholeStageCodegenIds(): Unit = {
    var currentCodegenId = -1

    def setCodegenId(p: LogicalPlan, children: Seq[QueryPlan[_]]): Unit = {
      if (currentCodegenId != -1) {
        p.setTagValue(QueryPlan.CODEGEN_ID_TAG, currentCodegenId)
      }
      children.foreach(_.generateWholeStageCodegenIds())
    }

    // // Skip the subqueries as they are not printed as part of main query block.
    // if (plan.isInstanceOf[BaseSubqueryExec]) {
    //   return
    // }
    foreach {
      // case p: WholeStageCodegenExec => currentCodegenId = p.codegenStageId
      // case _: InputAdapter => currentCodegenId = -1
      // case p: AdaptiveSparkPlanExec => setCodegenId(p, Seq(p.executedPlan))
      // case p: QueryStageExec => setCodegenId(p, Seq(p.plan))
      case other => setCodegenId(other, other.innerChildren)
    }
  }

  override protected def getSubqueries(
      subqueries: ArrayBuffer[(LogicalPlan, Expression, LogicalPlan)]): Unit = {
    foreach {
      // case a: AdaptiveSparkPlanExec =>
      //   getSubqueries(a.executedPlan, subqueries)
      // case p: SparkPlan =>
      //   p.expressions.foreach (_.collect {
      //     case e: PlanExpression[_] =>
      //       e.plan match {
      //         case s: BaseSubqueryExec =>
      //           subqueries += ((p, e, s))
      //           getSubqueries(s, subqueries)
      //         case _ =>
      //       }
      //   })
      p =>
        p.expressions.foreach (_.collect {
          case e: SubqueryExpression =>
            e.plan match {
              case s =>
                subqueries += ((p, e, s))
                s.getSubqueries(subqueries)
            }
        })
    }
  }

  override private[sql] def removeTags(): Unit = {
    def remove(p: LogicalPlan, children: Seq[QueryPlan[_]]): Unit = {
      p.unsetTagValue(QueryPlan.OP_ID_TAG)
      p.unsetTagValue(QueryPlan.CODEGEN_ID_TAG)
      children.foreach(_.removeTags())
    }

    foreach {
      // case p: AdaptiveSparkPlanExec => remove(p, Seq(p.executedPlan))
      // case p: QueryStageExec => remove(p, Seq(p.plan))
      case plan => remove(plan, plan.innerChildren)
    }
  }
}

/**
 * A logical plan node with no children.
 */
trait LeafNode extends LogicalPlan with LeafLike[LogicalPlan] {
  override def producedAttributes: AttributeSet = outputSet

  override def verboseStringWithOperatorId(): String = {
    val argumentString = argString(conf.maxToStringFields)
    val outputStr = s"${generateFieldString("Output", output)}"

    if (argumentString.nonEmpty) {
      s"""
         |$formattedNodeName
         |$outputStr
         |Arguments: $argumentString
         |""".stripMargin
    } else {
      s"""
         |$formattedNodeName
         |$outputStr
         |""".stripMargin
    }
  }

  /** Leaf nodes that can survive analysis must define their own statistics. */
  def computeStats(): Statistics = throw new UnsupportedOperationException
}

/**
 * A logical plan node with single child.
 */
trait UnaryNode extends LogicalPlan with UnaryLike[LogicalPlan] {
  /**
   * Generates all valid constraints including an set of aliased constraints by replacing the
   * original constraint expressions with the corresponding alias
   */
  protected def getAllValidConstraints(projectList: Seq[NamedExpression]): ExpressionSet = {
    var allConstraints = child.constraints
    projectList.foreach {
      case a @ Alias(l: Literal, _) =>
        allConstraints += EqualNullSafe(a.toAttribute, l)
      case a @ Alias(e, _) =>
        // For every alias in `projectList`, replace the reference in constraints by its attribute.
        allConstraints ++= allConstraints.map(_ transform {
          case expr: Expression if expr.semanticEquals(e) =>
            a.toAttribute
        })
        allConstraints += EqualNullSafe(e, a.toAttribute)
      case _ => // Don't change.
    }

    allConstraints
  }

  override protected lazy val validConstraints: ExpressionSet = child.constraints

  override def verboseStringWithOperatorId(): String = {
    val argumentString = argString(conf.maxToStringFields)
    val inputStr = s"${generateFieldString("Input", child.output)}"

    if (argumentString.nonEmpty) {
      s"""
         |$formattedNodeName
         |$inputStr
         |Arguments: $argumentString
         |""".stripMargin
    } else {
      s"""
         |$formattedNodeName
         |$inputStr
         |""".stripMargin
    }
  }
}

/**
 * A logical plan node with a left and right child.
 */
trait BinaryNode extends LogicalPlan with BinaryLike[LogicalPlan] {

  override def verboseStringWithOperatorId(): String = {
    val argumentString = argString(conf.maxToStringFields)
    val leftOutputStr = s"${generateFieldString("Left output", left.output)}"
    val rightOutputStr = s"${generateFieldString("Right output", right.output)}"

    if (argumentString.nonEmpty) {
      s"""
         |$formattedNodeName
         |$leftOutputStr
         |$rightOutputStr
         |Arguments: $argumentString
         |""".stripMargin
    } else {
      s"""
         |$formattedNodeName
         |$leftOutputStr
         |$rightOutputStr
         |""".stripMargin
    }
  }
}

abstract class OrderPreservingUnaryNode extends UnaryNode {
  override final def outputOrdering: Seq[SortOrder] = child.outputOrdering
}

object LogicalPlanIntegrity {

  private def canGetOutputAttrs(p: LogicalPlan): Boolean = {
    p.resolved && !p.expressions.exists { e =>
      e.collectFirst {
        // We cannot call `output` in plans with a `ScalarSubquery` expr having no column,
        // so, we filter out them in advance.
        case s: ScalarSubquery if s.plan.schema.fields.isEmpty => true
      }.isDefined
    }
  }

  /**
   * Since some logical plans (e.g., `Union`) can build `AttributeReference`s in their `output`,
   * this method checks if the same `ExprId` refers to attributes having the same data type
   * in plan output.
   */
  def hasUniqueExprIdsForOutput(plan: LogicalPlan): Boolean = {
    val exprIds = plan.collect { case p if canGetOutputAttrs(p) =>
      // NOTE: we still need to filter resolved expressions here because the output of
      // some resolved logical plans can have unresolved references,
      // e.g., outer references in `ExistenceJoin`.
      p.output.filter(_.resolved).map { a => (a.exprId, a.dataType.asNullable) }
    }.flatten

    val ignoredExprIds = plan.collect {
      // NOTE: `Union` currently reuses input `ExprId`s for output references, but we cannot
      // simply modify the code for assigning new `ExprId`s in `Union#output` because
      // the modification will make breaking changes (See SPARK-32741(#29585)).
      // So, this check just ignores the `exprId`s of `Union` output.
      case u: Union if u.resolved => u.output.map(_.exprId)
    }.flatten.toSet

    val groupedDataTypesByExprId = exprIds.filterNot { case (exprId, _) =>
      ignoredExprIds.contains(exprId)
    }.groupBy(_._1).values.map(_.distinct)

    groupedDataTypesByExprId.forall(_.length == 1)
  }

  /**
   * This method checks if reference `ExprId`s are not reused when assigning a new `ExprId`.
   * For example, it returns false if plan transformers create an alias having the same `ExprId`
   * with one of reference attributes, e.g., `a#1 + 1 AS a#1`.
   */
  def checkIfSameExprIdNotReused(plan: LogicalPlan): Boolean = {
    plan.collect { case p if p.resolved =>
      p.expressions.forall {
        case a: Alias =>
          // Even if a plan is resolved, `a.references` can return unresolved references,
          // e.g., in `Grouping`/`GroupingID`, so we need to filter out them and
          // check if the same `exprId` in `Alias` does not exist
          // among reference `exprId`s.
          !a.references.filter(_.resolved).map(_.exprId).exists(_ == a.exprId)
        case _ =>
          true
      }
    }.forall(identity)
  }

  /**
   * This method checks if the same `ExprId` refers to an unique attribute in a plan tree.
   * Some plan transformers (e.g., `RemoveNoopOperators`) rewrite logical
   * plans based on this assumption.
   */
  def checkIfExprIdsAreGloballyUnique(plan: LogicalPlan): Boolean = {
    checkIfSameExprIdNotReused(plan) && hasUniqueExprIdsForOutput(plan)
  }
}
