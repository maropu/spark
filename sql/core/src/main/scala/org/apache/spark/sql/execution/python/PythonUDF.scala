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

import org.apache.spark.api.python.PythonFunction
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Expression, NonSQLExpression, Unevaluable, UserDefinedExpression}
import org.apache.spark.sql.types.DataType

/**
 * A serialized version of a Python lambda function.
 */
case class PythonUDF(
    name: String,
    func: PythonFunction,
    dataType: DataType,
    children: Seq[Expression],
    pythonUdfType: Int,
    genFunc: Array[Byte] = Array.empty)
  extends Expression with Unevaluable with NonSQLExpression with UserDefinedExpression {

  override def toString: String = s"$name(${children.mkString(", ")})"

  override def nullable: Boolean = true
}

/**
 * A Python lambda function supporting whole-stage codegen.
 *
 * TODO: Most parts of code has the same with [[ScalaUDF]], so we need to merge both code.
 */
case class PythonCodegenUDF(
    name: String,
    genFuncName: String,
    func: Array[Byte],
    dataType: DataType,
    children: Seq[Expression])
  extends Expression with NonSQLExpression with UserDefinedExpression {

  override def toString: String = s"$name(${children.mkString(", ")})"

  override def nullable: Boolean = true

  override def eval(input: InternalRow): Any = {
    throw new UnsupportedOperationException(s"Cannot evaluate expression: $this")
  }

  def getChildren(): Seq[Expression] = children

  lazy val udfErrorMessage = {
    val funcCls = this.getClass.getSimpleName
    val inputTypes = children.map(_.dataType.simpleString).mkString(", ")
    s"Failed to execute user defined python function($funcCls: ($inputTypes) => " +
      s"${dataType.simpleString})"
  }

  // Generate codes used to convert the arguments to Scala type for user-defined Python functions
  private[this] def genCodeForConverter(ctx: CodegenContext, index: Int): String = {
    val converterClassName = classOf[Any => Any].getName
    val typeConvertersClassName = CatalystTypeConverters.getClass.getName + ".MODULE$"
    val expressionClassName = classOf[Expression].getName
    val scalaUDFClassName = classOf[PythonCodegenUDF].getName

    val converterTerm = ctx.freshName("converter")
    val expressionIdx = ctx.references.size - 1
    ctx.addMutableState(converterClassName, converterTerm,
      s"$converterTerm = ($converterClassName)$typeConvertersClassName" +
        s".createToScalaConverter(((${expressionClassName})((($scalaUDFClassName)" +
          s"references[$expressionIdx]).getChildren().apply($index))).dataType());")
    converterTerm
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val pythonUDF = ctx.addReferenceObj("pythonUDF", this)
    val converterClassName = classOf[Any => Any].getName
    val typeConvertersClassName = CatalystTypeConverters.getClass.getName + ".MODULE$"

    // Generate codes used to convert the returned value of user-defined python functions
    // to Catalyst type.
    val catalystConverterTerm = ctx.freshName("catalystConverter")
    ctx.addMutableState(converterClassName, catalystConverterTerm,
      s"$catalystConverterTerm = ($converterClassName)$typeConvertersClassName" +
        s".createToCatalystConverter($pythonUDF.dataType());")

    val resultTerm = ctx.freshName("result")

    // This must be called before children expressions' codegen
    // because ctx.references is used in genCodeForConverter
    val converterTerms = children.indices.map(genCodeForConverter(ctx, _))

    // codegen for children expressions
    val evals = children.map(_.genCode(ctx))

    // Generate the codes for expressions and calling user-defined function
    // We need to get the boxedType of dataType's javaType here. Because for the dataType
    // such as IntegerType, its javaType is `int` and the returned type of user-defined
    // function is Object. Trying to convert an Object to `int` will cause casting exception.
    val evalCode = evals.map(_.code).mkString
    val (converters, funcArguments) = converterTerms.zipWithIndex.map { case (converter, i) =>
      val eval = evals(i)
      val argTerm = ctx.freshName("arg")
      // TODO: Use hard-coded types for quick benchmarks
      val convert = s"java.lang.Number $argTerm = " +
        s"${eval.isNull} ? null : (java.lang.Number) $converter.apply(${eval.value});"
      (convert, s"$argTerm.longValue()")
    }.unzip

    val getFuncResult = s"GeneratedClass.$genFuncName(${funcArguments.mkString(", ")})"
    val callFunc =
      s"""
         ${ctx.boxedType(dataType)} $resultTerm = null;
         try {
           $resultTerm = (${ctx.boxedType(dataType)})$catalystConverterTerm.apply($getFuncResult);
         } catch (Exception e) {
           throw new org.apache.spark.SparkException($pythonUDF.udfErrorMessage(), e);
         }
       """

    ev.copy(code = s"""
      $evalCode
      ${converters.mkString("\n")}
      $callFunc

      boolean ${ev.isNull} = $resultTerm == null;
      ${ctx.javaType(dataType)} ${ev.value} = ${ctx.defaultValue(dataType)};
      if (!${ev.isNull}) {
        ${ev.value} = $resultTerm;
      }""")
  }
}
