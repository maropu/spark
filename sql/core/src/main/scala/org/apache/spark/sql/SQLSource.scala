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

import com.codahale.metrics.MetricRegistry

import org.apache.spark.metrics.source.Source

private[sql] class SQLSource(sparkSession: SparkSession) extends Source {
  override val metricRegistry = new MetricRegistry
  override val sourceName = "%s.SQLMetrics".format(
    sparkSession.sparkContext.appName.replace(" ", "_"))

  /**
   * Histograms of time spent running specific rules.
   */
  private val metricMap = {
    val analyzerRules = sparkSession.sessionState.analyzer.batches.flatMap(_.rules.map(_.ruleName))
    val optRules = sparkSession.sessionState.optimizer.batches.flatMap(_.rules.map(_.ruleName))
    (analyzerRules ++ optRules).map { ruleName =>
      ruleName -> metricRegistry.histogram(MetricRegistry.name(ruleName))
    }.toMap
  }

  def update(metricName: String, t: Long): Unit = {
    metricMap.get(metricName).foreach(_.update(t))
  }
}
