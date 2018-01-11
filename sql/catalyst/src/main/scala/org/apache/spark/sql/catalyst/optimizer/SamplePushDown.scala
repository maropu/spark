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

package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.expressions.And
import org.apache.spark.sql.catalyst.expressions.PredicateHelper
import org.apache.spark.sql.catalyst.planning.ExtractFiltersAndInnerJoins
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Sample}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.internal.SQLConf

/**
 * Pushes down [[Sample]] beneath the inputs of inner/outer joins under some conditions.
 * For example,
 */
object SamplePushDown extends Rule[LogicalPlan] with PredicateHelper {

  def apply(plan: LogicalPlan): LogicalPlan = plan.transform {
    case sample @ Sample(_, _, _, _, child) => child match {
      // TODO: Cover outer joins here
      case ExtractFiltersAndInnerJoins(input, conditions)
          if SQLConf.get.samplingPushDownEnabled && input.size > 2 && conditions.nonEmpty =>
        // Find the eligible star plans. Currently, it only returns
        // the star join with the largest fact table.
        val eligibleJoins = input.collect{ case (plan, Inner) => plan }
        val starJoinPlan = StarSchemaDetection.findStarJoins(eligibleJoins, conditions)
        if (starJoinPlan.nonEmpty) {
          val (factTable, dimTables) = (starJoinPlan.head, starJoinPlan.tail)
          val otherTables = input.filterNot { case (p, _) => starJoinPlan.contains(p) }
          // Gives up sampling pushdown if we have selective joins on dimension tables
          if (!StarSchemaDetection.isSelectiveStarJoin(dimTables, conditions)) {
            val factTableTableRefs = factTable.outputSet
            val (factTableConditions, otherConditions) = conditions.partition(
              e => e.references.subsetOf(factTableTableRefs) && canEvaluateWithinJoin(e))
            val newFactTable = if (factTableConditions.nonEmpty) {
              Filter(factTableConditions.reduceLeft(And), factTable)
            } else {
              factTable
            }
            val sampledFactTable = sample.copy(child = newFactTable)
            val newStarJoinPlan = (sampledFactTable +: dimTables).map(plan => (plan, Inner))
            ReorderJoin.createOrderedJoin(newStarJoinPlan ++ otherTables, otherConditions)
          } else {
            sample
          }
        } else {
          sample
        }
      case _ =>
        sample
    }
  }
}
