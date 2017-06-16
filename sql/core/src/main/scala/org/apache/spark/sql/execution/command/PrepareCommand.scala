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

package org.apache.spark.sql.execution.command

import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.catalyst.analysis.UnresolvedParameter
import org.apache.spark.sql.catalyst.errors.TreeNodeException
import org.apache.spark.sql.catalyst.expressions.ParameterPlaceHolder
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

case class ResolvePrepareParameters(paramTypes: Map[String, DataType]) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan.transformAllExpressions {
    case p @ UnresolvedParameter(name) =>
      assert(paramTypes.get(name).isDefined)
      ParameterPlaceHolder(name, paramTypes(name))
  }
}

case class PrepareCommand(identifier: String, preparedPlan: LogicalPlan, dataTypes: Seq[DataType])
    extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = try {
    if (preparedPlan.isStreaming) {
      throw new AnalysisException("Query having any streaming data sources not supported.")
    } else {
      if (sparkSession.sessionState.preparedStmtRegistration.lookupFunction(identifier).isDefined) {
        throw new AnalysisException(s"Prepared statement $identifier already exists")
      }
      val paramTypes = dataTypes.zipWithIndex.map { case (tpe, i) => s"${i + 1}" -> tpe }
      val paramHolders = preparedPlan.collect { case p =>
        p.expressions.flatMap(_.collect { case up: UnresolvedParameter => up.name })
      }.flatten
      if (paramTypes.map(_._1).toSet != paramHolders.toSet) {
        throw new AnalysisException (
          s"Names of given parameter types do not match names of parameter holders. " +
            s"Prepared statement name: $identifier; " +
            s"names of parameter types: ${paramTypes.map(s => s"`${s._1}`").mkString(", ")}; " +
            s"names of parameter holders: ${paramHolders.map(s => s"`$s`").mkString(", ")}.")
      }
      val planWithParamHolder = ResolvePrepareParameters(paramTypes.toMap).apply(preparedPlan)
      val analyzedPlan = sparkSession.sessionState.executePlan(planWithParamHolder).analyzed
      logDebug(s"identifier=$identifier query=$analyzedPlan")
      sparkSession.sessionState.preparedStmtRegistration.registerStatement(identifier, analyzedPlan)
    }
    Seq.empty[Row]
  } catch { case cause: TreeNodeException[_] =>
    ("Error occurred when analyzing a prepared statement: \n" + cause.getMessage)
      .split("\n").map(Row(_))
  }
}
