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

package org.apache.spark.sql

import java.io.File
import java.util.UUID

import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.Canonicalize
import org.scalatest.BeforeAndAfterAll
import org.apache.spark.sql.catalyst.expressions.{Alias, ExprId, Expression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeFormatter, CodeGenerator}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.util.Utils

abstract class BenchmarkQueryTest extends QueryTest with SharedSQLContext with BeforeAndAfterAll {

  // When Utils.isTesting is true, the RuleExecutor will issue an exception when hitting
  // the max iteration of analyzer/optimizer batches.
  assert(Utils.isTesting, "spark.testing is not set to true")

  private val regenerateGoldenFiles: Boolean = System.getenv("SPARK_GENERATE_GOLDEN_FILES") == "1"

  private val baseResourcePath = {
    // If regenerateGoldenFiles is true, we must be running this in SBT and we use hard-coded
    // relative path. Otherwise, we use classloader's getResource to find the location.
    val className = this.getClass.getSimpleName
    if (regenerateGoldenFiles) {
      java.nio.file.Paths.get(
        "src", "test", "resources", "benchmark-explain-results", className).toFile
    } else {
      val res = getClass.getClassLoader.getResource(s"benchmark-explain-results/$className")
      new File(res.getFile)
    }
  }

  /**
   * Drop all the tables
   */
  protected override def afterAll(): Unit = {
    try {
      // For debugging dump some statistics about how much time was spent in various optimizer rules
      logWarning(RuleExecutor.dumpTimeSpent())
      spark.sessionState.catalog.reset()
    } finally {
      super.afterAll()
    }
  }

  override def beforeAll() {
    super.beforeAll()
    RuleExecutor.resetMetrics()
  }

  protected def checkGeneratedCode(plan: SparkPlan): Unit = {
    val codegenSubtrees = new collection.mutable.HashSet[WholeStageCodegenExec]()
    plan foreach {
      case s: WholeStageCodegenExec =>
        codegenSubtrees += s
      case s => s
    }
    codegenSubtrees.toSeq.foreach { subtree =>
      val code = subtree.doCodeGen()._2
      try {
        // Just check the generated code can be properly compiled
        CodeGenerator.compile(code)
      } catch {
        case e: Exception =>
          val msg =
            s"""
               |failed to compile:
               |Subtree:
               |$subtree
               |Generated code:
               |${CodeFormatter.format(code)}
             """.stripMargin
          throw new Exception(msg, e)
      }
    }
  }

  private val dummyExprId = ExprId(0, UUID.nameUUIDFromBytes("dummyId".getBytes))

  private def canonicalizeExprs(p: SparkPlan): SparkPlan = p.transformAllExpressions {
    case ar: AttributeReference =>
      ar.withName(ar.name.replaceAll("""#\d+""", "#0")).withExprId(dummyExprId)
    case a: Alias =>
      a.copy(name = "none")(exprId = dummyExprId, None, None)
    case is: InSubquery =>
      val newSubquery = SubqueryExec("none", canonicalizePlans(is.plan))
      is.copy(plan = newSubquery, exprId = dummyExprId)
    case ss: ScalarSubquery =>
      val newSubquery = SubqueryExec("none", canonicalizePlans(ss.plan))
      ss.copy(plan = newSubquery, exprId = dummyExprId)
  }

  private def canonicalizeSparkPlans(p: SparkPlan): SparkPlan = p.transform {
    case f @ FilterExec(condition, _) =>
      val newCondition = condition.transformUp { case e => Canonicalize.expressionReorder(e) }
      f.copy(condition = newCondition)
    case p @ ProjectExec(proj, _) =>
      p.copy(projectList = proj.sortBy(_.hashCode()))
    case s @ SubqueryExec(name, _) =>
      s.copy(name = "none")
  }

  private def canonicalizePlans(p: SparkPlan): SparkPlan = {
    canonicalizeSparkPlans(canonicalizeExprs(p))
  }

  protected def checkExplainResults(df: DataFrame, queryName: String): Unit = {
    val plan = canonicalizePlans(df.queryExecution.executedPlan)
    val explainResult = plan.treeString(verbose = false)
      .replaceAll("""\*\(\d+\)\s""", "*")
      .replaceAll("file:\\/.*\\,", "<PATH>,")
    val resultFile = new File(baseResourcePath, s"$queryName.out")
    if (regenerateGoldenFiles) {
      resultFile.createNewFile()
      stringToFile(resultFile, explainResult)
    } else {
      val expectedExplainResult = fileToString(resultFile)
      assert(expectedExplainResult === explainResult)
    }
  }
}
