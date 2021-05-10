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

package org.apache.spark.sql.catalyst.plans

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions.Expression

trait ExplainPlanHelper[BaseType <: QueryPlan[BaseType]] {
  self: BaseType =>

  /**
   * Given a input physical plan, performs the following tasks.
   *   1. Computes the operator id for current operator and records it in the operator
   *      by setting a tag.
   *   2. Computes the whole stage codegen id for current operator and records it in the
   *      operator by setting a tag.
   *   3. Generate the two part explain output for this plan.
   *      1. First part explains the operator tree with each operator tagged with an unique
   *         identifier.
   *      2. Second part explains each operator in a verbose manner.
   *
   * Note : This function skips over subqueries. They are handled by its caller.
   *
   * @param plan Input query plan to process
   * @param append function used to append the explain output
   * @param startOperatorID The start value of operation id. The subsequent operations will
   *                         be assigned higher value.
   *
   * @return The last generated operation id for this input plan. This is to ensure we
   *         always assign incrementing unique id to each operator.
   *
   */
  protected def processPlanSkippingSubqueries(append: String => Unit, startOperatorID: Int): Int = {
    val operationIDs = new mutable.ArrayBuffer[(Int, BaseType)]()
    var currentOperatorID = startOperatorID
    try {
      currentOperatorID = generateOperatorIDs(currentOperatorID, operationIDs)
      generateWholeStageCodegenIds()

      QueryPlan.append(
        this,
        append,
        verbose = false,
        addSuffix = false,
        printOperatorId = true)

      append("\n")
      var i: Integer = 0
      for ((opId, curPlan) <- operationIDs) {
        append(curPlan.verboseStringWithOperatorId())
      }
    } catch {
      case e: AnalysisException => append(e.toString)
    }
    currentOperatorID
  }

  /**
   * Given a input physical plan, performs the following tasks.
   *   1. Generates the explain output for the input plan excluding the subquery plans.
   *   2. Generates the explain output for each subquery referenced in the plan.
   */
  private[sql] def processPlan(append: String => Unit): Unit

  /**
   * Traverses the supplied input plan in a bottom-up fashion does the following :
   *    1. produces a map : operator identifier -> operator
   *    2. Records the operator id via setting a tag in the operator.
   * Note :
   *    1. Operator such as WholeStageCodegenExec and InputAdapter are skipped as they don't
   *       appear in the explain output.
   *    2. operator identifier starts at startOperatorID + 1
   * @param plan Input query plan to process
   * @param startOperatorID The start value of operation id. The subsequent operations will
   *                         be assigned higher value.
   * @param operatorIDs A output parameter that contains a map of operator id and query plan. This
   *                    is used by caller to print the detail portion of the plan.
   * @return The last generated operation id for this input plan. This is to ensure we
   *         always assign incrementing unique id to each operator.
   */
  protected def generateOperatorIDs(
    startOperatorID: Int,
    operatorIDs: mutable.ArrayBuffer[(Int, BaseType)]): Int

  /**
   * Traverses the supplied input plan in a top-down fashion and records the
   * whole stage code gen id in the plan via setting a tag.
   */
  private[sql] def generateWholeStageCodegenIds(): Unit

  /**
   * Generate detailed field string with different format based on type of input value
   */
  protected def generateFieldString(fieldName: String, values: Any): String = values match {
    case iter: Iterable[_] if (iter.size == 0) => s"${fieldName}: []"
    case iter: Iterable[_] => s"${fieldName} [${iter.size}]: ${iter.mkString("[", ", ", "]")}"
    case str: String if (str == null || str.isEmpty) => s"${fieldName}: None"
    case str: String => s"${fieldName}: ${str}"
    case _ => throw new IllegalArgumentException(s"Unsupported type for argument values: $values")
  }

  /**
   * Given a input plan, returns an array of tuples comprising of :
   *  1. Hosting operator id.
   *  2. Hosting expression
   *  3. Subquery plan
   */
  protected def getSubqueries(subqueries: ArrayBuffer[(BaseType, Expression, BaseType)]): Unit

  /**
   * Returns the operator identifier for the supplied plan by retrieving the
   * `operationId` tag value.
   */
  protected def getOpId(): String = {
    this.getTagValue(QueryPlan.OP_ID_TAG).map(v => s"$v").getOrElse("unknown")
  }

  private[sql] def removeTags(): Unit
}
