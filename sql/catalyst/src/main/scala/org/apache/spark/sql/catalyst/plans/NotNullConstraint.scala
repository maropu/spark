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

import org.apache.spark.sql.catalyst.expressions.{Attribute, ExprId, Expression, IsNotNull, NullIntolerant}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan


trait NotNullConstraint { self: LogicalPlan =>

  final def toPlanWithNotNullConstraint: LogicalPlan = NotNullConstraintPlanVisitor.visit(self)

  final def outputWithNotNullConstraint: Seq[Attribute] = output.map { a =>
    if (a.nullable && notNullAttributes.contains(a.exprId)) {
      a.withNullability(false)
    } else {
      a
    }
  }

  // If one expression and its children are null intolerant, it is null intolerant
  private def isNullIntolerant(expr: Expression): Boolean = expr match {
    case e: NullIntolerant => e.children.forall(isNullIntolerant)
    case _ => false
  }

  // Holds the `ExprId` set of not-NULL attributes in this logical plan
  private lazy val notNullAttributes = constraints.flatMap {
    case isnotnull @ IsNotNull(a) if isNullIntolerant(a) => isnotnull.references.map(_.exprId)
    case _ => Seq.empty[ExprId]
  }.toSet
}
