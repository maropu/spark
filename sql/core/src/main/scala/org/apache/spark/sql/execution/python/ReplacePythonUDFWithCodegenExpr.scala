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

package org.apache.spark.sql.execution.python

import java.lang.{Long => jLong}

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import maropu.lljvm.{LLJVMClassLoader, LLJVMUtils}

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.util.Utils


/**
 * Replaces [[PythonUDF]]s with UDF expressions supporting codegen.
 */
object ReplacePythonUDFWithCodegenExpr extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan.transformAllExpressions {
    case pyFunc @ PythonUDF(name, _, dataType, children, _, genFunc) if genFunc.nonEmpty =>
      try {
        // TODO: Needs to strictly check if we can compile python functions, e.g.,
        // checking if we can build JVM bytecode from LLVM bitcode and load the bytecode in JVMs.
        val loader = new LLJVMClassLoader(Utils.getContextOrSparkClassLoader)
        val clazz = loader.loadClassFromBitcode("GeneratedClass", genFunc)
        val genFuncName = {
          // TODO: Needs to look for a qualified function name by using children types.
          // Now, we just use hard-coded types for quick benchmarks.
          val method = LLJVMUtils.findMethods(clazz, name, jLong.TYPE, jLong.TYPE)
          method.asScala.headOption.map(_.getName).get
        }
        PythonCodegenUDF(name, genFuncName, genFunc, dataType, children)
      } catch {
        case NonFatal(_) =>
          // If we can't compile it, we keep as it is
          pyFunc
      }
  }
}
