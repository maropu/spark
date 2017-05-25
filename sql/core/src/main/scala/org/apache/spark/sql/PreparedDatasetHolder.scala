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

import java.sql.Date
import java.sql.Timestamp

import scala.language.implicitConversions
import scala.reflect.runtime.universe.TypeTag

import org.apache.commons.lang3.StringUtils

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.annotation.InterfaceStability
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, ScalaReflection}
import org.apache.spark.sql.catalyst.analysis.TypeCoercion.ImplicitTypeCasts
import org.apache.spark.sql.catalyst.encoders._
import org.apache.spark.sql.catalyst.expressions.{Cast, Literal, ParameterHolder}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, ReturnAnswer}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.{LogicalRDD, QueryExecution, SQLExecution}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types.StructType


class PreparedDatasetHolder[T] private[sql](
    @DeveloperApi @InterfaceStability.Unstable @transient queryExecution: QueryExecution,
    @transient optimizedPlanMayHaveParamHolder: LogicalPlan,
    encoder: Encoder[T]) extends Serializable {

  private def sparkSession = queryExecution.sparkSession
  private def planner = sparkSession.sessionState.planner

  private def bindParamInPlan[U: TypeTag](name: String, value: U, plan: LogicalPlan)
    : LogicalPlan = {
    val paramType = ScalaReflection.schemaFor[U].dataType
    val binder: Rule[LogicalPlan] = new Rule[LogicalPlan] {
      override def apply(plan: LogicalPlan): LogicalPlan = plan.transformAllExpressions {
        case Cast(param @ ParameterHolder(paramName), expectedType, _) if paramName == name =>
          val catalystValue = CatalystTypeConverters.createToCatalystConverter(paramType)(value)
          val paramExpr = Literal(catalystValue, paramType)
          ImplicitTypeCasts.implicitCast(paramExpr, expectedType).getOrElse {
            throw new AnalysisException(
              s"Cannot bind a param $value in $param because $paramType is incompatible " +
                s"with expected one($expectedType)")
          }
      }
    }
    binder(plan)
  }

  def bindParam[U: TypeTag](paramName: String, value: U): PreparedDatasetHolder[T] = {
    val plan = bindParamInPlan(paramName, value, optimizedPlanMayHaveParamHolder)
    new PreparedDatasetHolder[T](queryExecution, plan, encoder)
  }


  /**
   * A code snippet below is just copyed from [[Dataset]], so we need to do something
   * to remove code duplication.
   */

  /**
   * Returns the schema of this [[PreparedDatasetHolder]].
   */
  def schema: StructType = queryExecution.analyzed.schema

  private implicit def classTag = exprEnc.clsTag

  private def executedPlan: SparkPlan = {
    val plan = {
      SparkSession.setActiveSession(sparkSession)
      planner.plan(ReturnAnswer(optimizedPlanMayHaveParamHolder)).next()
    }
    queryExecution.prepareForExecution(plan)
  }

  private def toDF(): DataFrame = {
    // TODO: Need to check if all the [[ParameterHolder]]s are replaced with actual values
    val logicalRdd = LogicalRDD(schema.toAttributes, executedPlan.execute())(sparkSession)
    val newQueryExecution = new QueryExecution(sparkSession, logicalRdd)
    new Dataset[Row](sparkSession, newQueryExecution, RowEncoder(schema))
  }

  private implicit val exprEnc: ExpressionEncoder[T] = encoderFor(encoder)
  private val boundEnc =
    exprEnc.resolveAndBind(optimizedPlanMayHaveParamHolder.output, sparkSession.sessionState.analyzer)

  private def collectFromPlan(plan: SparkPlan): Array[T] = {
    plan.executeCollect().map(boundEnc.fromRow).toArray[T]
  }

  private def withAction[U](name: String, qe: QueryExecution)(action: SparkPlan => U) = {
    val plan = executedPlan
    try {
      plan.foreach { plan =>
        plan.resetMetrics()
      }
      val start = System.nanoTime()
      val result = SQLExecution.withNewExecutionId(sparkSession, qe) {
        action(plan)
      }
      val end = System.nanoTime()
      sparkSession.listenerManager.onSuccess(name, qe, end - start)
      result
    } catch {
      case e: Exception =>
        sparkSession.listenerManager.onFailure(name, qe, e)
        throw e
    }
  }

  /**
   * Returns an array that contains all rows in this Dataset.
   */
  def collect(): Array[T] = withAction("collect", queryExecution)(collectFromPlan)

  /**
   * Displays the Dataset in a tabular form.
   */
  // scalastyle:off println
  def show(): Unit =
    println(showString(20, 20, false))
  // scalastyle:on println

  /**
   * Compose the string representing rows for output
   */
  private def showString(
      _numRows: Int, truncate: Int = 20, vertical: Boolean = false): String = {
    val numRows = _numRows.max(0)
    val takeResult = toDF().take(numRows + 1)
    val hasMoreData = takeResult.length > numRows
    val data = takeResult.take(numRows)

    lazy val timeZone =
      DateTimeUtils.getTimeZone(sparkSession.sessionState.conf.sessionLocalTimeZone)

    // For array values, replace Seq and Array with square brackets
    // For cells that are beyond `truncate` characters, replace it with the
    // first `truncate-3` and "..."
    val rows: Seq[Seq[String]] = schema.fieldNames.toSeq +: data.map { row =>
      row.toSeq.map { cell =>
        val str = cell match {
          case null => "null"
          case binary: Array[Byte] => binary.map("%02X".format(_)).mkString("[", " ", "]")
          case array: Array[_] => array.mkString("[", ", ", "]")
          case seq: Seq[_] => seq.mkString("[", ", ", "]")
          case d: Date =>
            DateTimeUtils.dateToString(DateTimeUtils.fromJavaDate(d))
          case ts: Timestamp =>
            DateTimeUtils.timestampToString(DateTimeUtils.fromJavaTimestamp(ts), timeZone)
          case _ => cell.toString
        }
        if (truncate > 0 && str.length > truncate) {
          // do not show ellipses for strings shorter than 4 characters.
          if (truncate < 4) str.substring(0, truncate)
          else str.substring(0, truncate - 3) + "..."
        } else {
          str
        }
      }: Seq[String]
    }

    val sb = new StringBuilder
    val numCols = schema.fieldNames.length
    // We set a minimum column width at '3'
    val minimumColWidth = 3

    if (!vertical) {
      // Initialise the width of each column to a minimum value
      val colWidths = Array.fill(numCols)(minimumColWidth)

      // Compute the width of each column
      for (row <- rows) {
        for ((cell, i) <- row.zipWithIndex) {
          colWidths(i) = math.max(colWidths(i), cell.length)
        }
      }

      // Create SeparateLine
      val sep: String = colWidths.map("-" * _).addString(sb, "+", "+", "+\n").toString()

      // column names
      rows.head.zipWithIndex.map { case (cell, i) =>
        if (truncate > 0) {
          StringUtils.leftPad(cell, colWidths(i))
        } else {
          StringUtils.rightPad(cell, colWidths(i))
        }
      }.addString(sb, "|", "|", "|\n")

      sb.append(sep)

      // data
      rows.tail.foreach {
        _.zipWithIndex.map { case (cell, i) =>
          if (truncate > 0) {
            StringUtils.leftPad(cell.toString, colWidths(i))
          } else {
            StringUtils.rightPad(cell.toString, colWidths(i))
          }
        }.addString(sb, "|", "|", "|\n")
      }

      sb.append(sep)
    } else {
      // Extended display mode enabled
      val fieldNames = rows.head
      val dataRows = rows.tail

      // Compute the width of field name and data columns
      val fieldNameColWidth = fieldNames.foldLeft(minimumColWidth) { case (curMax, fieldName) =>
        math.max(curMax, fieldName.length)
      }
      val dataColWidth = dataRows.foldLeft(minimumColWidth) { case (curMax, row) =>
        math.max(curMax, row.map(_.length).reduceLeftOption[Int] { case (cellMax, cell) =>
          math.max(cellMax, cell)
        }.getOrElse(0))
      }

      dataRows.zipWithIndex.foreach { case (row, i) =>
        // "+ 5" in size means a character length except for padded names and data
        val rowHeader = StringUtils.rightPad(
          s"-RECORD $i", fieldNameColWidth + dataColWidth + 5, "-")
        sb.append(rowHeader).append("\n")
        row.zipWithIndex.map { case (cell, j) =>
          val fieldName = StringUtils.rightPad(fieldNames(j), fieldNameColWidth)
          val data = StringUtils.rightPad(cell, dataColWidth)
          s" $fieldName | $data "
        }.addString(sb, "", "\n", "\n")
      }
    }

    // Print a footer
    if (vertical && data.isEmpty) {
      // In a vertical mode, print an empty row set explicitly
      sb.append("(0 rows)\n")
    } else if (hasMoreData) {
      // For Data that has more than "numRows" records
      val rowsString = if (numRows == 1) "row" else "rows"
      sb.append(s"only showing top $numRows $rowsString\n")
    }

    sb.toString()
  }
}
