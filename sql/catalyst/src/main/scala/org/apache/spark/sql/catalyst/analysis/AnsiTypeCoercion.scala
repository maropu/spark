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

import javax.annotation.Nullable

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.Cast._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.types._

object AnsiTypeCoercion {

  def canImplicitCast(from: DataType, to: DataType): Boolean = (from, to) match {
    case (ByteType, ShortType | IntegerType | LongType | _: FractionalType | _: DecimalType) => true
    case (ShortType, IntegerType | LongType | _: FractionalType | _: DecimalType) => true
    case (IntegerType, LongType | _: FractionalType | _: DecimalType) => true
    case (LongType, _: FractionalType | _: DecimalType) => true
    case (FloatType, DoubleType) => true
    case (_: DecimalType, _: FractionalType) => true
    case (DateType, TimestampType) => true
    case (NullType, _) => true

    case (ArrayType(fromType, fn), ArrayType(toType, tn)) =>
      canImplicitCast(fromType, toType) &&
        resolvableNullability(fn || forceNullable(fromType, toType), tn)

    case (MapType(fromKey, fromValue, fn), MapType(toKey, toValue, tn)) =>
      canImplicitCast(fromKey, toKey) &&
        (!forceNullable(fromKey, toKey)) &&
        canCast(fromValue, toValue) &&
        resolvableNullability(fn || forceNullable(fromValue, toValue), tn)

    case (StructType(fromFields), StructType(toFields)) =>
      fromFields.length == toFields.length &&
        fromFields.zip(toFields).forall {
          case (fromField, toField) =>
            canImplicitCast(fromField.dataType, toField.dataType) &&
              resolvableNullability(
                fromField.nullable || forceNullable(fromField.dataType, toField.dataType),
                toField.nullable)
        }

    case (udt1: UserDefinedType[_], udt2: UserDefinedType[_]) if udt1.userClass == udt2.userClass =>
      true

    case _ => false
  }

  private def canAssignmentCastOnly(from: DataType, to: DataType): Boolean = (from, to) match {
    case (ShortType, ByteType) => true
    case (IntegerType, ByteType | ShortType) => true
    case (LongType, ByteType | ShortType | IntegerType) => true
    case (FloatType, _: NumericType | _: DecimalType) => true
    case (DoubleType, _: NumericType | FloatType | _: DecimalType) => true
    case (_: DecimalType, _: NumericType) => true
    case (DateType, TimestampType) => true
    case (TimestampType, DateType) => true
    case (_, StringType) => true
    case _ => false
  }

  def canAssignmentCast(from: DataType, to: DataType): Boolean = {
    canImplicitCast(from, to) || canAssignmentCastOnly(from, to)
  }

  private def buildCastWithValueRange(
      child: Expression,
      dataType: DataType,
      timeZoneId: Option[String],
      valueRange: (Literal, Literal)): Expression = {
    val minCheckPred = GreaterThanOrEqual(child, valueRange._1)
    val maxCheckPred = GreaterThanOrEqual(valueRange._2, child)
    val ifExpr = If(And(minCheckPred, maxCheckPred),
      Cast(child, dataType, timeZoneId),
      Literal(null, dataType))

    ifExpr.transform {
      case gte @ GreaterThanOrEqual(left, right) if !left.dataType.sameType(right.dataType) =>
        TypeCoercion.findWiderTypeForTwo(left.dataType, right.dataType).map { widestType =>
          GreaterThanOrEqual(
            Cast(left, widestType, timeZoneId),
            Cast(right, widestType, timeZoneId)
          )
        }.getOrElse(gte)
    }
  }

  def castWithValueRangeCheck(
      child: Expression,
      dataType: DataType,
      timeZoneId: Option[String] = None): Expression = {
    if (!child.dataType.sameType(dataType)) {
      val valueRangeOption = dataType match {
        case ByteType =>
          Some(Seq(Byte.MinValue, Byte.MaxValue).map(Literal(_, ByteType)))
        case ShortType =>
          Some(Seq(Short.MinValue, Short.MaxValue).map(Literal(_, ShortType)))
        case IntegerType =>
          Some(Seq(Int.MinValue, Int.MaxValue).map(Literal(_, IntegerType)))
        case LongType =>
          Some(Seq(Long.MinValue, Long.MaxValue).map(Literal(_, LongType)))
        case FloatType =>
          Some(Seq(Float.MinValue, Float.MaxValue).map(Literal(_, FloatType)))
        case DoubleType =>
          Some(Seq(Double.MinValue, Double.MaxValue).map(Literal(_, DoubleType)))
        case _ =>
          None
      }
      valueRangeOption.map { case Seq(minValue, maxValue) =>
        val inputExpr = (child.dataType, dataType) match {
          case (_: FractionalType, _: IntegralType) => Round(child, Literal(0))
          case _ => child
        }
        buildCastWithValueRange(inputExpr, dataType, timeZoneId, (minValue, maxValue))
      }.getOrElse(
        Cast(child, dataType, timeZoneId)
      )
    } else {
      child
    }
  }

  /**
   * If `spark.sql.ansi.typeCoercion.enabled`=true, casts types according to the expected input
   * types for [[Expression]]s based on the ANSI SQL-like type coercion rule.
   */
  object ImplicitTypeCasts {

    private def implicitCast(e: Expression, expectedType: AbstractDataType): Option[Expression] = {
      implicitCast(e.dataType, expectedType).map { dt =>
        if (dt == e.dataType) e else Cast(e, dt)
      }
    }

    private def implicitCast(inType: DataType, expectedType: AbstractDataType): Option[DataType] = {
      // Note that ret is nullable to avoid typing a lot of Some(...) in this local scope.
      // We wrap immediately an Option after this.
      @Nullable val ret: DataType = (inType, expectedType) match {
        // If the expected type is already a parent of the input type, no need to cast.
        case _ if expectedType.acceptsType(inType) => inType

        // Cast null type (usually from null literals) into target types
        case (NullType, target) => target.defaultConcreteType

        // Implicit cast among numeric types. When we reach here, input type is not acceptable.

        case (FloatType | DoubleType, DecimalType) => null

        // If input is a numeric type but not decimal, and we expect a decimal type,
        // cast the input to decimal.
        case (d: NumericType, DecimalType) => DecimalType.forType(d)

        // implicitly cast to numeric types with precedence
        case (in: NumericType, target: NumericType) if canImplicitCast(in, target) => target

        // Implicit cast from date to timestamp only
        case (DateType, TimestampType) => TimestampType

        // When we reach here, input type is not acceptable for any types in this type collection,
        // try to find the first one we can implicitly cast.
        case (_, TypeCollection(types)) =>
          types.flatMap(implicitCast(inType, _)).headOption.orNull

        // Implicit cast between array types.
        //
        // Compare the nullabilities of the from type and the to type, check whether the cast of
        // the nullability is resolvable by the following rules:
        // 1. If the nullability of the to type is true, the cast is always allowed;
        // 2. If the nullability of the to type is false, and the nullability of the from type is
        // true, the cast is never allowed;
        // 3. If the nullabilities of both the from type and the to type are false, the cast is
        // allowed only when Cast.forceNullable(fromType, toType) is false.
        case (ArrayType(fromType, fn), ArrayType(toType: DataType, true)) =>
          implicitCast(fromType, toType).map(ArrayType(_, true)).orNull

        case (ArrayType(fromType, true), ArrayType(toType: DataType, false)) => null

        case (ArrayType(fromType, false), ArrayType(toType: DataType, false))
            if !Cast.forceNullable(fromType, toType) =>
          implicitCast(fromType, toType).map(ArrayType(_, false)).orNull

        // Implicit cast between Map types.
        // Follows the same semantics of implicit casting between two array types.
        // Refer to documentation above. Make sure that both key and values
        // can not be null after the implicit cast operation by calling forceNullable
        // method.
        case (MapType(fromKeyType, fromValueType, fn), MapType(toKeyType, toValueType, tn))
            if !Cast.forceNullable(fromKeyType, toKeyType) && Cast.resolvableNullability(fn, tn) =>
          if (Cast.forceNullable(fromValueType, toValueType) && !tn) {
            null
          } else {
            val newKeyType = implicitCast(fromKeyType, toKeyType).orNull
            val newValueType = implicitCast(fromValueType, toValueType).orNull
            if (newKeyType != null && newValueType != null) {
              MapType(newKeyType, newValueType, tn)
            } else {
              null
            }
          }

        case _ => null
      }
      Option(ret)
    }

    private val findResultType: (DataType, DataType) => Option[DataType] = {
      case (t1, t2) if t1 == t2 => Some(t1)
      case (NullType, t1) => Some(t1)
      case (t1, NullType) => Some(t1)

      case (t1: IntegralType, t2: DecimalType) if t2.isWiderThan(t1) =>
        Some(t2)
      case (t1: DecimalType, t2: IntegralType) if t1.isWiderThan(t2) =>
        Some(t1)

      case (_: NumericType, FloatType | DoubleType) =>
        Some(DoubleType)
      case (FloatType | DoubleType, _: NumericType) =>
        Some(DoubleType)

      // Promote numeric types to the highest of the two
      case (t1: NumericType, t2: NumericType)
          if !t1.isInstanceOf[DecimalType] && !t2.isInstanceOf[DecimalType] =>
        val index = TypeCoercion.numericPrecedence.lastIndexWhere(t => t == t1 || t == t2)
        Some(TypeCoercion.numericPrecedence(index))

      case (_: TimestampType, _: DateType) | (_: DateType, _: TimestampType) =>
        Some(TimestampType)

      case (t1, t2) => TypeCoercion.findTypeForComplex(t1, t2, findResultType)
    }

    private[sql] def coerceTypes(
        plan: LogicalPlan): LogicalPlan = plan resolveExpressions {
      // Skip nodes who's children have not been resolved yet.
      case e if !e.childrenResolved => e

      case b @ BinaryOperator(left, right) if left.dataType != right.dataType =>
        findResultType(left.dataType, right.dataType).map { commonType =>
          if (b.inputType.acceptsType(commonType)) {
            // If the expression accepts the result type, cast to that.
            val newLeft = if (left.dataType == commonType) left else Cast(left, commonType)
            val newRight = if (right.dataType == commonType) right else Cast(right, commonType)
            b.withNewChildren(Seq(newLeft, newRight))
          } else {
            // Otherwise, don't do anything with the expression.
            b
          }
        }.getOrElse(b)  // If there is no applicable conversion, leave expression unchanged.

      case e: ImplicitCastInputTypes if e.inputTypes.nonEmpty =>
        val children: Seq[Expression] = e.children.zip(e.inputTypes).map { case (in, expected) =>
          // If we cannot do the implicit cast, just use the original input.
          implicitCast(in, expected).getOrElse(in)
        }
        e.withNewChildren(children)

      case e: ExpectsInputTypes if e.inputTypes.nonEmpty =>
        // Convert NullType into some specific target type for ExpectsInputTypes that don't do
        // general implicit casting.
        val children: Seq[Expression] = e.children.zip(e.inputTypes).map { case (in, expected) =>
          if (in.dataType == NullType && !expected.acceptsType(NullType)) {
            Literal.create(null, expected.defaultConcreteType)
          } else {
            in
          }
        }
        e.withNewChildren(children)

      case udf: ScalaUDF if udf.inputTypes.nonEmpty =>
        val children = udf.children.zip(udf.inputTypes).map { case (in, expected) =>
          // Currently Scala UDF will only expect `AnyDataType` at top level, so this trick works.
          // In the future we should create types like `AbstractArrayType`, so that Scala UDF can
          // accept inputs of array type of arbitrary element type.
          if (expected == AnyDataType) {
            in
          } else {
            implicitCast(
              in,
              TypeCoercion.ImplicitTypeCasts.udfInputToCastType(
                in.dataType, expected.asInstanceOf[DataType])
            ).getOrElse(in)
          }

        }
        udf.withNewChildren(children)
    }
  }
}
