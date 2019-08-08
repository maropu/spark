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

package org.apache.spark.sql.jdbc

import java.sql.Timestamp

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCPartition, JDBCRelation}
import org.apache.spark.sql.test.SharedSQLContext

class CalciteJDBCSuite extends QueryTest with SharedSQLContext {

  // Test data should be defined in `test.org.apache.spark.sql.jdbc.CalciteTestData`
  val url = {
    val modelPath = getClass.getResource("/calcite-test-model.json").getPath
    s"jdbc:calcite:model=$modelPath;timeZone=GMT"
  }

  private def urlWithLex(lex: String) = s"$url;lex=$lex"

  // `Oracle` and `MYSQL_ANSI` passed, but the others failed
  Seq("ORACLE", "MYSQL", "MYSQL_ANSI", "SQL_SERVER").foreach { lexMode =>
    test(s"timestamp types in partitionColumn (lex=$lexMode)") {
      val df = spark.read.format("jdbc")
        .option("url", urlWithLex(lexMode))
        .option("dbtable", "testdb.foo")
        .option("partitionColumn", "TIMES")
        .option("lowerBound", "2019-01-01 00:00:00")
        .option("upperBound", "2019-08-09 00:00:00")
        .option("numPartitions", 2)
        .load()

      df.logicalPlan match {
        case LogicalRelation(JDBCRelation(_, parts, _), _, _, _) =>
          val whereClauses = parts.map(_.asInstanceOf[JDBCPartition].whereClause).toSet
          assert(whereClauses === Set(
            """"TIMES" < '2019-04-21 00:30:00' or "TIMES" is null""",
            """"TIMES" >= '2019-04-21 00:30:00'"""))
      }

      val expectedResult = Seq(
        ("AAA", "2019-01-02 00:00:00"),
        ("BBB", "2019-03-05 00:00:00"),
        ("CCC", "2019-08-01 00:00:00")
      ).map { case (id, times) =>
        Row(id, Timestamp.valueOf(times))
      }
      checkAnswer(df, expectedResult)
    }
  }
}
