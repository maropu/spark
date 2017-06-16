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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Literal, ParameterPlaceHolder}
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LocalRelationWithParamPlaceHolder, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{StructField, StructType}

/**
 * An analyzer rule that replaces [[UnresolvedInlineTable]] with [[LocalRelation]].
 */
case class ResolveInlineTables(conf: SQLConf) extends Rule[LogicalPlan] with CastSupport {

  private def hasParamHolder(table: UnresolvedInlineTable): Boolean =
    table.rows.flatten.exists(_.isInstanceOf[ParameterPlaceHolder])

  override def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
    case table: UnresolvedInlineTable if table.expressionsResolved && hasParamHolder(table) =>
      validateInputDimension(table)
      validateInputEvaluable(table)
      convertToLocalRelationWithParamPlaceHolder(table)

    case table: UnresolvedInlineTable if table.expressionsResolved =>
      validateInputDimension(table)
      validateInputEvaluable(table)
      convertToLocalRelation(table)
  }

  /**
   * Validates the input data dimension:
   * 1. All rows have the same cardinality.
   * 2. The number of column aliases defined is consistent with the number of columns in data.
   *
   * This is package visible for unit testing.
   */
  private[analysis] def validateInputDimension(table: UnresolvedInlineTable): Unit = {
    if (table.rows.nonEmpty) {
      val numCols = table.names.size
      table.rows.zipWithIndex.foreach { case (row, ri) =>
        if (row.size != numCols) {
          table.failAnalysis(s"expected $numCols columns but found ${row.size} columns in row $ri")
        }
      }
    }
  }

  /**
   * Validates that all inline table data are valid expressions that can be evaluated
   * (in this they must be foldable).
   *
   * This is package visible for unit testing.
   */
  private[analysis] def validateInputEvaluable(table: UnresolvedInlineTable): Unit = {
    table.rows.foreach { row =>
      row.foreach { e =>
        // Note that nondeterministic expressions are not supported since they are not foldable.
        if (!e.resolved || !e.foldable) {
          e.failAnalysis(s"cannot evaluate expression ${e.sql} in inline table definition")
        }
      }
    }
  }

  /**
   * Create output [[Attribute]]s from given expressions in `table`.
   */
  private[analysis] def toAttributes(table: UnresolvedInlineTable): Seq[Attribute] = {
    // For each column, traverse all the values and find a common data type and nullability.
    val fields = table.rows.transpose.zip(table.names).map { case (column, name) =>
      val inputTypes = column.map(_.dataType)
      val tpe = TypeCoercion.findWiderTypeWithoutStringPromotion(inputTypes).getOrElse {
        table.failAnalysis(s"incompatible types found in column $name for inline table")
      }
      StructField(name, tpe, nullable = column.exists(_.nullable))
    }
    val attributes = StructType(fields).toAttributes
    assert(fields.size == table.names.size)
    attributes
  }

  /**
   * Convert a valid (with right shape and foldable inputs) [[UnresolvedInlineTable]]
   * into a [[LocalRelation]].
   *
   * This function attempts to coerce inputs into consistent types.
   *
   * This is package visible for unit testing.
   */
  private[analysis] def convertToLocalRelation(table: UnresolvedInlineTable): LocalRelation = {
    val attributes = toAttributes(table)
    val newRows = table.rows.map { row =>
      InternalRow.fromSeq(row.zip(attributes.map(_.dataType)).map { case (e, targetType) =>
        try {
          mayCast(e, targetType).eval()
        } catch {
          case NonFatal(ex) =>
            table.failAnalysis(s"failed to evaluate expression ${e.sql}: ${ex.getMessage}")
        }
      })
    }
    LocalRelation(attributes, newRows)
  }

  /**
   * Convert a valid [[UnresolvedInlineTable]] into a [[LocalRelationWithParamPlaceHolder]].
   */
  private[analysis] def convertToLocalRelationWithParamPlaceHolder(table: UnresolvedInlineTable)
    : LocalRelationWithParamPlaceHolder = {
    val attributes = toAttributes(table)
    val newRows = table.rows.map { row =>
      row.zip(attributes.map(_.dataType)).map {
        case (ph: ParameterPlaceHolder, targetType) =>
          mayCast(ph, targetType)
        case (e, targetType) =>
          try {
            // Check if we can safely evaluate this in `ResolvePreparedStatement`
            Literal(mayCast(e, targetType).eval(), targetType)
          } catch {
            case NonFatal(ex) =>
              table.failAnalysis(s"failed to evaluate expression ${e.sql}: ${ex.getMessage}")
          }
      }
    }
    LocalRelationWithParamPlaceHolder(attributes, newRows)
  }
}
