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

package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.analysis.EliminateSubqueryAliases
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{AttributeMap, Expression}
import org.apache.spark.sql.catalyst.planning.ExtractFiltersAndInnerJoins
import org.apache.spark.sql.catalyst.plans.{Cross, Inner, InnerLike, PlanTest}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.catalyst.statsEstimation.StatsTestPlan
import org.apache.spark.sql.internal.SQLConf

class JoinOptimizationSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Subqueries", Once,
        EliminateSubqueryAliases) ::
      Batch("Filter Pushdown", FixedPoint(100),
        CombineFilters,
        PushDownPredicate,
        BooleanSimplification,
        ReorderJoin,
        PushPredicateThroughJoin,
        ColumnPruning,
        CollapseProject) :: Nil

  }

  val testRelation = LocalRelation('a.int, 'b.int, 'c.int)
  val testRelation1 = LocalRelation('d.int)

  def testExtractInnerJoins(
      plan: LogicalPlan,
      expected: Option[(Seq[(LogicalPlan, InnerLike)], Seq[Expression])]) {
    ExtractFiltersAndInnerJoins.unapply(plan) match {
      case Some((input, conditions)) =>
        expected.map { case (expectedPlans, expectedConditions) =>
          assert(expectedPlans === input)
          assert(expectedConditions.toSet === conditions.toSet)
        }
      case None =>
        assert(expected.isEmpty)
    }
  }

  test("extract filters and joins") {
    val x = testRelation.subquery('x)
    val y = testRelation1.subquery('y)
    val z = testRelation.subquery('z)

    def testExtract(plan: LogicalPlan, expected: Option[(Seq[LogicalPlan], Seq[Expression])]) {
      val expectedNoCross = expected map {
        seq_pair => {
          val plans = seq_pair._1
          val noCartesian = plans map { plan => (plan, Inner) }
          (noCartesian, seq_pair._2)
        }
      }
      testExtractInnerJoins(plan, expectedNoCross)
    }

    testExtract(x, None)
    testExtract(x.where("x.b".attr === 1), None)
    testExtract(x.join(y), Some((Seq(x, y), Seq())))
    testExtract(x.join(y, condition = Some("x.b".attr === "y.d".attr)),
      Some((Seq(x, y), Seq("x.b".attr === "y.d".attr))))
    testExtract(x.join(y).where("x.b".attr === "y.d".attr),
      Some((Seq(x, y), Seq("x.b".attr === "y.d".attr))))
    testExtract(x.join(y).join(z), Some((Seq(x, y, z), Seq())))
    testExtract(x.join(y).where("x.b".attr === "y.d".attr).join(z),
      Some((Seq(x, y, z), Seq("x.b".attr === "y.d".attr))))
    testExtract(x.join(y).join(x.join(z)), Some((Seq(x, y, x.join(z)), Seq())))
    testExtract(x.join(y).join(x.join(z)).where("x.b".attr === "y.d".attr),
      Some((Seq(x, y, x.join(z)), Seq("x.b".attr === "y.d".attr))))

    testExtractInnerJoins(x.join(y, Cross), Some((Seq((x, Cross), (y, Cross)), Seq())))
    testExtractInnerJoins(x.join(y, Cross).join(z, Cross),
      Some((Seq((x, Cross), (y, Cross), (z, Cross)), Seq())))
    testExtractInnerJoins(x.join(y, Cross, Some("x.b".attr === "y.d".attr)).join(z, Cross),
      Some((Seq((x, Cross), (y, Cross), (z, Cross)), Seq("x.b".attr === "y.d".attr))))
    testExtractInnerJoins(x.join(y, Inner, Some("x.b".attr === "y.d".attr)).join(z, Cross),
      Some((Seq((x, Inner), (y, Inner), (z, Cross)), Seq("x.b".attr === "y.d".attr))))
    testExtractInnerJoins(x.join(y, Cross, Some("x.b".attr === "y.d".attr)).join(z, Inner),
      Some((Seq((x, Cross), (y, Cross), (z, Inner)), Seq("x.b".attr === "y.d".attr))))
  }

  test("reorder inner joins") {
    val x = testRelation.subquery('x)
    val y = testRelation1.subquery('y)
    val z = testRelation.subquery('z)

    val queryAnswers = Seq(
      (
        x.join(y).join(z).where(("x.b".attr === "z.b".attr) && ("y.d".attr === "z.a".attr)),
        x.join(z, condition = Some("x.b".attr === "z.b".attr))
          .join(y, condition = Some("y.d".attr === "z.a".attr))
      ),
      (
        x.join(y, Cross).join(z, Cross)
          .where(("x.b".attr === "z.b".attr) && ("y.d".attr === "z.a".attr)),
        x.join(z, Cross, Some("x.b".attr === "z.b".attr))
          .join(y, Cross, Some("y.d".attr === "z.a".attr))
      ),
      (
        x.join(y, Inner).join(z, Cross).where("x.b".attr === "z.a".attr),
        x.join(z, Cross, Some("x.b".attr === "z.a".attr)).join(y, Inner)
      )
    )

    queryAnswers foreach { queryAnswerPair =>
      val optimized = Optimize.execute(queryAnswerPair._1.analyze) match {
        // `ReorderJoin` may add `Project` to keep the same order of output attributes.
        // So, we drop a top `Project` for tests.
        case project: Project => project.child
        case p => p
      }
      comparePlans(optimized, queryAnswerPair._2.analyze)
    }
  }

  test("broadcasthint sets relation statistics to smallest value") {
    val input = LocalRelation('key.int, 'value.string)

    val query =
      Project(Seq($"x.key", $"y.key"),
        Join(
          SubqueryAlias("x", input),
          ResolvedHint(SubqueryAlias("y", input)), Cross, None)).analyze

    val optimized = Optimize.execute(query)

    val expected =
      Join(
        Project(Seq($"x.key"), SubqueryAlias("x", input)),
        ResolvedHint(Project(Seq($"y.key"), SubqueryAlias("y", input))),
        Cross, None).analyze

    comparePlans(optimized, expected)

    val broadcastChildren = optimized.collect {
      case Join(_, r, _, _) if r.stats.sizeInBytes == 1 => r
    }
    assert(broadcastChildren.size == 1)
  }

  test("SPARK-23172 skip projections when flattening joins") {
    val x = testRelation.subquery('x)
    val y = testRelation1.subquery('y)
    val z = testRelation.subquery('z)
    val joined = x.join(z, Inner, Some($"x.b" === $"z.b")).select($"x.a", $"z.a", $"z.c")
      .join(y, Inner, Some($"y.d" === $"z.a")).analyze
    val expectedTables = joined.collectLeaves().map { case p => (p, Inner) }
    val expectedConditions = joined.collect { case Join(_, _, _, Some(conditions)) => conditions }
    testExtractInnerJoins(joined, Some((expectedTables, expectedConditions)))
  }

  test("SPARK-23172 reorder joins with projections") {
    withSQLConf(
        SQLConf.STARSCHEMA_DETECTION.key -> "true",
        SQLConf.CBO_ENABLED.key -> "false") {
      val r0output = Seq('a.int, 'b.int, 'c.int)
      val r0colStat = ColumnStat(distinctCount = Some(100000000), nullCount = Some(0))
      val r0colStats = AttributeMap(r0output.map(_ -> r0colStat))
      val r0 = StatsTestPlan(r0output, 100000000, r0colStats, name = Some("r0")).subquery('r0)

      val r1output = Seq('a.int, 'd.int)
      val r1colStat = ColumnStat(distinctCount = Some(10), nullCount = Some(0))
      val r1colStats = AttributeMap(r1output.map(_ -> r1colStat))
      val r1 = StatsTestPlan(r1output, 10, r1colStats, name = Some("r1")).subquery('r1)

      val r2output = Seq('b.int, 'e.int)
      val r2colStat = ColumnStat(distinctCount = Some(100), nullCount = Some(0))
      val r2colStats = AttributeMap(r2output.map(_ -> r2colStat))
      val r2 = StatsTestPlan(r2output, 100, r2colStats, name = Some("r2")).subquery('r2)

      val r3output = Seq('c.int, 'f.int)
      val r3colStat = ColumnStat(distinctCount = Some(1), nullCount = Some(0))
      val r3colStats = AttributeMap(r3output.map(_ -> r3colStat))
      val r3 = StatsTestPlan(r3output, 1, r3colStats, name = Some("r3")).subquery('r3)

      val joined = r0.join(r1, Inner, Some($"r0.a" === $"r1.a" && $"r1.d" >= 3))
        .select($"r0.b", $"r0.c", $"r1.d")
        .join(r2, Inner, Some($"r0.b" === $"r2.b" && $"r2.e" >= 5))
        .select($"r0.c", $"r1.d", $"r2.e")
        .join(r3, Inner, Some($"r0.c" === $"r3.c" && $"r3.f" <= 100))
        .select($"r1.d", $"r2.e", $"r3.f")
        .analyze

      val optimized = Optimize.execute(joined)
      val optJoins = ReorderJoin.extractLeftDeepInnerJoins(optimized)
      val joinOrder = optJoins.flatMap(_.collect{ case p: StatsTestPlan => p }.headOption)
        .flatMap(_.name)
      assert(joinOrder === Seq("r2", "r1", "r3", "r0"))
    }
  }
}
