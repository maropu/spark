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

import java.util.Locale
import javax.annotation.concurrent.GuardedBy

import scala.collection.mutable

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
 * A catalog for looking up registered statements, used by an [[Analyzer]].
 */
class PreparedStatementRegistry {

  @GuardedBy("this")
  private val preparedStatements = new mutable.HashMap[String, LogicalPlan]

  // Resolution of the statement name is always case insensitive
  private def normalizeStmtName(stmtName: String): String= {
    stmtName.toLowerCase(Locale.ROOT)
  }

  def registerStatement(name: String, analyzedLogicalPlan: LogicalPlan): Unit = {
    preparedStatements.put(normalizeStmtName(name), analyzedLogicalPlan)
  }

  def lookupFunction(stmtName: String): Option[LogicalPlan] = synchronized {
    preparedStatements.get(normalizeStmtName(stmtName))
  }

  def dropFunction(stmtName: String): Boolean = synchronized {
    preparedStatements.remove(normalizeStmtName(stmtName)).isDefined
  }

  def clear(): Unit = synchronized {
    preparedStatements.clear()
  }

  override def clone(): PreparedStatementRegistry = synchronized {
    val registry = new PreparedStatementRegistry
    preparedStatements.iterator.foreach { case (stmtName, plan) =>
      registry.registerStatement(stmtName, plan)
    }
    registry
  }
}

object EmptyPreparedStatementRegistry extends PreparedStatementRegistry {

  override def registerStatement(name: String, analyzedLogicalPlan: LogicalPlan): Unit = {
    throw new UnsupportedOperationException
  }

  override def lookupFunction(stmtName: String): Option[LogicalPlan] = {
    throw new UnsupportedOperationException
  }

  override def dropFunction(stmtName: String): Boolean = {
    throw new UnsupportedOperationException
  }

  override def clear(): Unit = {
    throw new UnsupportedOperationException
  }
}
