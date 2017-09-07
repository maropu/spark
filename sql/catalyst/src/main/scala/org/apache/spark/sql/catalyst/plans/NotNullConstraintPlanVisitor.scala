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

package org.apache.spark.sql.catalyst.plans

import org.apache.spark.sql.catalyst.expressions.{AttributeMap, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical._


object NotNullConstraintPlanVisitor extends LogicalPlanVisitor[LogicalPlan] {

  override def default(p: LogicalPlan): LogicalPlan = p match {
    case p if !p.isInstanceOf[LeafNode] =>
      val children = p.children.map(_.toPlanWithNotNullConstraint)
      val childrenOutput = p.children.flatMap(_.outputWithNotNullConstraint)
      val nullabilityMap = AttributeMap(childrenOutput.map { x => x -> x.nullable })
      val newPlan = p.transformExpressions {
        case ar: AttributeReference =>
          nullabilityMap.get(ar).filterNot(_ == ar.nullable).map { nullable =>
            ar.withNullability(nullable)
          }.getOrElse(ar)
      }
      newPlan.withNewChildren(children)
    case leafNode =>
      leafNode
  }

  override def visitUnion(p: Union): LogicalPlan = default(p)
  override def visitGlobalLimit(p: GlobalLimit): LogicalPlan = default(p)
  override def visitAggregate(p: Aggregate): LogicalPlan = default(p)
  override def visitExcept(p: Except): LogicalPlan = default(p)
  override def visitDistinct(p: Distinct): LogicalPlan = default(p)
  override def visitSample(p: Sample): LogicalPlan = default(p)
  override def visitRepartitionByExpr(p: RepartitionByExpression): LogicalPlan = default(p)
  override def visitFilter(p: Filter): LogicalPlan = default(p)
  override def visitWindow(p: Window): LogicalPlan = default(p)
  override def visitGenerate(p: Generate): LogicalPlan = default(p)
  override def visitIntersect(p: Intersect): LogicalPlan = default(p)
  override def visitScriptTransform(p: ScriptTransformation): LogicalPlan = default(p)
  override def visitRepartition(p: Repartition): LogicalPlan = default(p)
  override def visitJoin(p: Join): LogicalPlan = default(p)
  override def visitLocalLimit(p: LocalLimit): LogicalPlan = default(p)
  override def visitProject(p: Project): LogicalPlan = default(p)
  override def visitExpand(p: Expand): LogicalPlan = default(p)
  override def visitPivot(p: Pivot): LogicalPlan = default(p)
  override def visitHint(p: ResolvedHint): LogicalPlan = default(p)
}
