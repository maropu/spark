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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types._

/**
 * A placeholder expression for cube/rollup, which will be replaced by analyzer
 */
trait GroupingSet extends Expression with CodegenFallback {

  def groupByExprs: Seq[Expression]
  override def children: Seq[Expression] = groupByExprs

  // this should be replaced first
  override lazy val resolved: Boolean = false

  override def dataType: DataType = throw new UnsupportedOperationException
  override def foldable: Boolean = false
  override def nullable: Boolean = true
  override def eval(input: InternalRow): Any = throw new UnsupportedOperationException
}

// scalastyle:off line.size.limit line.contains.tab
@ExpressionDescription(
  usage = """
    _FUNC_([col1[, col2 ..]]) - create a multi-dimensional cube using the specified columns
      so that we can run aggregation on them.
  """,
  examples = """
    Examples:
      > SELECT name, age, count(*) FROM VALUES (2, 'Alice'), (5, 'Bob') people(age, name) GROUP BY _FUNC_(name, age);
        Bob	5	1
        Alice	2	1
        NULL	NULL	2
        NULL	5	1
        Bob	NULL	1
        Alice	NULL	1
        NULL	2	1
  """,
  since = "2.0.0")
// scalastyle:on line.size.limit line.contains.tab
case class Cube(groupByExprs: Seq[Expression]) extends GroupingSet {}

// scalastyle:off line.size.limit line.contains.tab
@ExpressionDescription(
  usage = """
    _FUNC_([col1[, col2 ..]]) - create a multi-dimensional rollup using the specified columns
      so that we can run aggregation on them.
  """,
  examples = """
    Examples:
      > SELECT name, age, count(*) FROM VALUES (2, 'Alice'), (5, 'Bob') people(age, name) GROUP BY _FUNC_(name, age);
        Bob	5	1
        Alice	2	1
        NULL	NULL	2
        Bob	NULL	1
        Alice	NULL	1
  """,
  since = "2.0.0")
// scalastyle:on line.size.limit line.contains.tab
case class Rollup(groupByExprs: Seq[Expression]) extends GroupingSet {}

/**
 * Indicates whether a specified column expression in a GROUP BY list is aggregated or not.
 * GROUPING returns 1 for aggregated or 0 for not aggregated in the result set.
 */
// scalastyle:off line.size.limit line.contains.tab
@ExpressionDescription(
  usage = """
    _FUNC_(col) - indicates whether a specified column in a GROUP BY is aggregated or
      not, returns 1 for aggregated or 0 for not aggregated in the result set.",
  """,
  examples = """
    Examples:
      > SELECT name, _FUNC_(name), sum(age) FROM VALUES (2, 'Alice'), (5, 'Bob') people(age, name) GROUP BY cube(name);
        Bob	0	5
        Alice	0	2
        NULL	1	7
  """,
  since = "2.0.0")
// scalastyle:on line.size.limit line.contains.tab
case class Grouping(child: Expression) extends Expression with Unevaluable {
  @transient
  override lazy val references: AttributeSet =
    AttributeSet(VirtualColumn.groupingIdAttribute :: Nil)
  override def children: Seq[Expression] = child :: Nil
  override def dataType: DataType = ByteType
  override def nullable: Boolean = false
}

/**
 * GroupingID is a function that computes the level of grouping.
 *
 * If groupByExprs is empty, it means all grouping expressions in GroupingSets.
 */
// scalastyle:off line.size.limit line.contains.tab
@ExpressionDescription(
  usage = """
    _FUNC_([col1[, col2 ..]]) - returns the level of grouping, equals to
      `(grouping(c1) << (n-1)) + (grouping(c2) << (n-2)) + ... + grouping(cn)`
  """,
  examples = """
    Examples:
      > SELECT name, _FUNC_(), sum(age), avg(height) FROM VALUES (2, 'Alice', 165), (5, 'Bob', 180) people(age, name, height) GROUP BY cube(name, height);
        NULL	2	5	180.0
        Alice	0	2	165.0
        NULL	3	7	172.5
        NULL	2	2	165.0
        Bob	1	5	180.0
        Alice	1	2	165.0
        Bob	0	5	180.0
  """,
  note = """
    Input columns should match with grouping columns exactly, or empty (means all the grouping
    columns).
  """,
  since = "2.0.0")
// scalastyle:on line.size.limit line.contains.tab
case class GroupingID(groupByExprs: Seq[Expression], outputDataType: Option[DataType])
    extends Expression with Unevaluable {

  def this(groupByExprs: Seq[Expression]) = this(groupByExprs, None)

  @transient
  override lazy val references: AttributeSet =
    AttributeSet(VirtualColumn.groupingIdAttribute :: Nil)
  // `ResolveGroupingAnalytics` resolves `groupByExprs` if it is empty
  override lazy val resolved: Boolean =
    childrenResolved && groupByExprs.nonEmpty && outputDataType.isDefined
  override def children: Seq[Expression] = groupByExprs
  override def dataType: DataType = outputDataType.get
  override def nullable: Boolean = false
  override def prettyName: String = "grouping_id"
}

object GroupingID {

  val MAX_GROUPING_NUM_FOR_INTEGER_ID = 31

  // Construct a whole group-by list from selected group-by lists
  def constructGroupBy(selectedGroupByExprs: Seq[Seq[Expression]]): Seq[Expression] = {
    selectedGroupByExprs.flatten.foldLeft(Seq.empty[Expression]) { (result, currentExpr) =>
      // Only unique expressions are included in the group by expressions and is determined
      // based on their semantic equality. Example. grouping sets ((a * b), (b * a)) results
      // in grouping expression (a * b)
      if (result.find(_.semanticEquals(currentExpr)).isDefined) {
        result
      } else {
        result :+ currentExpr
      }
    }
  }

  def getFormat(selectedGroupByExprs: Seq[Seq[Expression]]): (DataType, Boolean) = {
    val groupByExprs = constructGroupBy(selectedGroupByExprs)
    val withIndex = selectedGroupByExprs.size != selectedGroupByExprs.map(_.toSet).toSet.size
    // We have three cases below for grouping IDs when having GroupBy attributes (a, b, c, d):
    //   1) `5` and `1` for grouping sets ((a, c), (a, b, c))
    //   2) `"0101"` and `"0001"` for grouping sets ((a, c), (a, b, c))
    //   3) `"0111-0"` and `"0111-1"` for grouping sets ((a), (a))
    if (!withIndex) {
      if (GroupingID.MAX_GROUPING_NUM_FOR_INTEGER_ID >= groupByExprs.size) {
        (IntegerType, false)
      } else {
        (StringType, false)
      }
    } else {
      (StringType, true)
    }
  }

  def apply(selectedGroupByExprs: Seq[Seq[Expression]]): GroupingID = {
    val groupByExprs = constructGroupBy(selectedGroupByExprs)
    val (dataType, _) = getFormat(selectedGroupByExprs)
    GroupingID(groupByExprs, Some(dataType))
  }
}
