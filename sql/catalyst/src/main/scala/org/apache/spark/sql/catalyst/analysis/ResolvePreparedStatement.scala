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

package org.apache.spark.sql.catalyst.analysis

import scala.util.control.NonFatal

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.catalyst.expressions.ParameterPlaceHolder
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LocalRelationWithParamPlaceHolder, LogicalPlan}
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.internal.SQLConf

case class ResolvePreparedStatement(conf: SQLConf, catalog: SessionCatalog)
    extends Rule[LogicalPlan] with CastSupport {

  private val stmtRegistration = catalog.stmtRegistration

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperators {
    case table @ LocalRelationWithParamPlaceHolder(names, rows) =>
      val newRows = rows.map { row =>
        InternalRow.fromSeq(row.map { case e =>
          try { e.eval() } catch {
            case NonFatal(ex) =>
              table.failAnalysis(s"failed to evaluate expression ${e.sql}: ${ex.getMessage}")
          }
        })
      }
      LocalRelation(table.output, newRows)

    case u @ UnresolvedPreparedStatement(identifier, params) =>
      val exprMap = params.zipWithIndex.map { case (expr, i) => s"${i + 1}" -> expr }.toMap
      stmtRegistration.lookupFunction(identifier).map { _.transformAllExpressions {
        case ph @ ParameterPlaceHolder(name, targetType) =>
          val e = exprMap.getOrElse(name, {
            throw new AnalysisException(
              s"Parameter `$$$name` not defined in a prepared statement: $plan")
          })
          mayCast(e, targetType)
        }
      }.getOrElse {
        throw new AnalysisException(s"Prepared statement `$identifier` does not exist")
      }
  }
}
