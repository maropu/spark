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

package org.apache.spark.sql.catalyst.plans.logical.statsEstimation

import org.apache.spark.sql.catalyst.expressions.AttributeMap
import org.apache.spark.sql.catalyst.plans.logical.{ColumnStat, Statistics, Union}
import org.apache.spark.sql.types._

/**
 * Estimate the number of output rows by doing the sum of output rows for each child of union,
 * and estimate min and max stats for each column by finding the overall min and max of that
 * column coming from its children.
 */
object UnionEstimation {
  import EstimationUtils._

  private def supportedType(dt: DataType): Boolean = {
    case NumericType => true
    case DateType | TimestampType => true
    case _ => false
  }

  private def createStatComparator(dt: DataType) = dt match {
    case ByteType => (a: Any, b: Any) =>
      ByteType.ordering.lt(a.asInstanceOf[Byte], b.asInstanceOf[Byte])
    case ShortType => (a: Any, b: Any) =>
      ShortType.ordering.lt(a.asInstanceOf[Short], b.asInstanceOf[Short])
    // Lists up all the supported types...
    case _ =>
      throw new IllegalStateException(s"Unsupported data type: ${dt.catalogString}")
  }

  def estimate(union: Union): Option[Statistics] = {
    val sizeInBytes = union.children.map(_.stats.sizeInBytes).sum
    val outputRows = if (rowCountsExist(union.children: _*)) {
      Some(union.children.map(_.stats.rowCount.get).sum)
    } else {
      None
    }

    val childrenToComputeMinMaxStats = union.children.map(_.output)
        .transpose.zipWithIndex.filter { case (attrs, _) =>
      supportedType(attrs.head.dataType) &&
        // Checks if all the children have min/max stats
        attrs.zipWithIndex.forall { case (attr, childIndex) =>
          val attrStats = union.children(childIndex).stats.attributeStats
          attrStats.get(attr).isDefined && attrStats(attr).hasMinMaxStats
        }
    }

    val newAttrStats = if (childrenToComputeMinMaxStats.nonEmpty) {
      val unionOutput = union.output
      val statComparators = childrenToComputeMinMaxStats.map { c =>
        createStatComparator(c._1.head.dataType)
      }
      val outputAttrStats = childrenToComputeMinMaxStats.zip(statComparators)
          .map { case ((attrs, outputIndex), statComparator) =>
        val minMaxValue = attrs.zipWithIndex.foldLeft[(Option[Any], Option[Any])]((None, None)) {
          case ((minVal, maxVal), (attr, childIndex)) =>
            val colStat = union.children(childIndex).stats.attributeStats(attr)
            val min = if (statComparator(colStat.min.get, minVal.get)) {
              colStat.min
            } else {
              minVal
            }
            val max = if (statComparator(maxVal.get, colStat.max.get)) {
              colStat.max
            } else {
              maxVal
            }
            (min, max)
        }
        val newStat = ColumnStat(min = minMaxValue._1, max = minMaxValue._2)
        unionOutput(outputIndex) -> newStat
      }
      AttributeMap(outputAttrStats.toSeq)
    } else {
      AttributeMap.empty[ColumnStat]
    }

    Some(Statistics(
      sizeInBytes = sizeInBytes,
      rowCount = outputRows,
      attributeStats = newAttrStats))
  }
}
