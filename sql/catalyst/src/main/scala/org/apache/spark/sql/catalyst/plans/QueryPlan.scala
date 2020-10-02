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

import scala.collection.mutable

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.trees.{CurrentOrigin, TreeNode, TreeNodeTag}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, StructType}

/**
 * An abstraction of the Spark SQL query plan tree, which can be logical or physical. This class
 * defines some basic properties of a query plan node, as well as some new transform APIs to
 * transform the expressions of the plan node.
 *
 * Note that, the query plan is a mutually recursive structure:
 *   QueryPlan -> Expression (subquery) -> QueryPlan
 * The tree traverse APIs like `transform`, `foreach`, `collect`, etc. that are
 * inherited from `TreeNode`, do not traverse into query plans inside subqueries.
 */
abstract class QueryPlan[PlanType <: QueryPlan[PlanType]] extends TreeNode[PlanType] {
  self: PlanType =>

  /**
   * The active config object within the current scope.
   * See [[SQLConf.get]] for more information.
   */
  def conf: SQLConf = SQLConf.get

  def output: Seq[Attribute]

  /**
   * Returns the set of attributes that are output by this node.
   */
  @transient
  lazy val outputSet: AttributeSet = AttributeSet(output)

  /**
   * The set of all attributes that are input to this operator by its children.
   */
  def inputSet: AttributeSet =
    AttributeSet(children.flatMap(_.asInstanceOf[QueryPlan[PlanType]].output))

  /**
   * The set of all attributes that are produced by this node.
   */
  def producedAttributes: AttributeSet = AttributeSet.empty

  /**
   * All Attributes that appear in expressions from this operator.  Note that this set does not
   * include attributes that are implicitly referenced by being passed through to the output tuple.
   */
  @transient
  lazy val references: AttributeSet = {
    AttributeSet.fromAttributeSets(expressions.map(_.references)) -- producedAttributes
  }

  /**
   * Attributes that are referenced by expressions but not provided by this node's children.
   */
  final def missingInput: AttributeSet = references -- inputSet

  /**
   * Runs [[transformExpressionsDown]] with `rule` on all expressions present
   * in this query operator.
   * Users should not expect a specific directionality. If a specific directionality is needed,
   * transformExpressionsDown or transformExpressionsUp should be used.
   *
   * @param rule the rule to be applied to every expression in this operator.
   */
  def transformExpressions(rule: PartialFunction[Expression, Expression]): this.type = {
    transformExpressionsDown(rule)
  }

  /**
   * Runs [[transformDown]] with `rule` on all expressions present in this query operator.
   *
   * @param rule the rule to be applied to every expression in this operator.
   */
  def transformExpressionsDown(rule: PartialFunction[Expression, Expression]): this.type = {
    mapExpressions(_.transformDown(rule))
  }

  /**
   * Runs [[transformUp]] with `rule` on all expressions present in this query operator.
   *
   * @param rule the rule to be applied to every expression in this operator.
   * @return
   */
  def transformExpressionsUp(rule: PartialFunction[Expression, Expression]): this.type = {
    mapExpressions(_.transformUp(rule))
  }

  /**
   * Apply a map function to each expression present in this query operator, and return a new
   * query operator based on the mapped expressions.
   */
  def mapExpressions(f: Expression => Expression): this.type = {
    var changed = false

    @inline def transformExpression(e: Expression): Expression = {
      val newE = CurrentOrigin.withOrigin(e.origin) {
        f(e)
      }
      if (newE.fastEquals(e)) {
        e
      } else {
        changed = true
        newE
      }
    }

    def recursiveTransform(arg: Any): AnyRef = arg match {
      case e: Expression => transformExpression(e)
      case Some(value) => Some(recursiveTransform(value))
      case m: Map[_, _] => m
      case d: DataType => d // Avoid unpacking Structs
      case stream: Stream[_] => stream.map(recursiveTransform).force
      case seq: Iterable[_] => seq.map(recursiveTransform)
      case other: AnyRef => other
      case null => null
    }

    val newArgs = mapProductIterator(recursiveTransform)

    if (changed) makeCopy(newArgs).asInstanceOf[this.type] else this
  }

  /**
   * Returns the result of running [[transformExpressions]] on this node
   * and all its children. Note that this method skips expressions inside subqueries.
   */
  def transformAllExpressions(rule: PartialFunction[Expression, Expression]): this.type = {
    transform {
      case q: QueryPlan[_] => q.transformExpressions(rule).asInstanceOf[PlanType]
    }.asInstanceOf[this.type]
  }

  /** Returns all of the expressions present in this query plan operator. */
  final def expressions: Seq[Expression] = {
    // Recursively find all expressions from a traversable.
    def seqToExpressions(seq: Iterable[Any]): Iterable[Expression] = seq.flatMap {
      case e: Expression => e :: Nil
      case s: Iterable[_] => seqToExpressions(s)
      case other => Nil
    }

    productIterator.flatMap {
      case e: Expression => e :: Nil
      case s: Some[_] => seqToExpressions(s.toSeq)
      case seq: Iterable[_] => seqToExpressions(seq)
      case other => Nil
    }.toSeq
  }

  /**
   * A variant of `transformUp`, which takes care of the case that the rule replaces a plan node
   * with a new one that has different output expr IDs, by updating the attribute references in
   * the parent nodes accordingly.
   *
   * @param rule the function to transform plan nodes, and return new nodes with attributes mapping
   *             from old attributes to new attributes. The attribute mapping will be used to
   *             rewrite attribute references in the parent nodes.
   * @param skipCond a boolean condition to indicate if we can skip transforming a plan node to save
   *                 time.
   */
  def transformUpWithNewOutput(
      rule: PartialFunction[PlanType, (PlanType, Seq[(Attribute, Attribute)])],
      skipCond: PlanType => Boolean = _ => false): PlanType = {
    def rewrite(plan: PlanType): (PlanType, Seq[(Attribute, Attribute)]) = {
      if (skipCond(plan)) {
        plan -> Nil
      } else {
        val attrMapping = new mutable.ArrayBuffer[(Attribute, Attribute)]()
        var newPlan = plan.mapChildren { child =>
          val (newChild, childAttrMapping) = rewrite(child)
          attrMapping ++= childAttrMapping
          newChild
        }

        val attrMappingForCurrentPlan = attrMapping.filter {
          // The `attrMappingForCurrentPlan` is used to replace the attributes of the
          // current `plan`, so the `oldAttr` must be part of `plan.references`.
          case (oldAttr, _) => plan.references.contains(oldAttr)
        }

        val (planAfterRule, newAttrMapping) = CurrentOrigin.withOrigin(origin) {
          rule.applyOrElse(newPlan, (plan: PlanType) => plan -> Nil)
        }
        newPlan = planAfterRule

        if (attrMappingForCurrentPlan.nonEmpty) {
          assert(!attrMappingForCurrentPlan.groupBy(_._1.exprId)
            .exists(_._2.map(_._2.exprId).distinct.length > 1),
            "Found duplicate rewrite attributes")

          val attributeRewrites = AttributeMap(attrMappingForCurrentPlan.toSeq)
          // Using attrMapping from the children plans to rewrite their parent node.
          // Note that we shouldn't rewrite a node using attrMapping from its sibling nodes.
          newPlan = newPlan.transformExpressions {
            case a: AttributeReference =>
              updateAttr(a, attributeRewrites)
            case pe: PlanExpression[PlanType] =>
              pe.withNewPlan(updateOuterReferencesInSubquery(pe.plan, attributeRewrites))
          }
        }

        attrMapping ++= newAttrMapping.filter {
          case (a1, a2) => a1.exprId != a2.exprId
        }
        newPlan -> attrMapping.toSeq
      }
    }
    rewrite(this)._1
  }

  private def updateAttr(attr: Attribute, attrMap: AttributeMap[Attribute]): Attribute = {
    val exprId = attrMap.getOrElse(attr, attr).exprId
    attr.withExprId(exprId)
  }

  /**
   * The outer plan may have old references and the function below updates the
   * outer references to refer to the new attributes.
   */
  private def updateOuterReferencesInSubquery(
      plan: PlanType,
      attrMap: AttributeMap[Attribute]): PlanType = {
    plan.transformDown { case currentFragment =>
      currentFragment.transformExpressions {
        case OuterReference(a: AttributeReference) =>
          OuterReference(updateAttr(a, attrMap))
        case pe: PlanExpression[PlanType] =>
          pe.withNewPlan(updateOuterReferencesInSubquery(pe.plan, attrMap))
      }
    }
  }

  lazy val schema: StructType = StructType.fromAttributes(output)

  /** Returns the output schema in the tree format. */
  def schemaString: String = schema.treeString

  /** Prints out the schema in the tree format */
  // scalastyle:off println
  def printSchema(): Unit = println(schemaString)
  // scalastyle:on println

  /**
   * A prefix string used when printing the plan.
   *
   * We use "!" to indicate an invalid plan, and "'" to indicate an unresolved plan.
   */
  protected def statePrefix = if (missingInput.nonEmpty && children.nonEmpty) "!" else ""

  override def simpleString(maxFields: Int): String = statePrefix + super.simpleString(maxFields)

  override def verboseString(maxFields: Int): String = simpleString(maxFields)

  override def simpleStringWithNodeId(): String = {
    val operatorId = getTagValue(QueryPlan.OP_ID_TAG).map(id => s"$id").getOrElse("unknown")
    s"$nodeName ($operatorId)".trim
  }

  def verboseStringWithOperatorId(): String = {
    val argumentString = argString(SQLConf.get.maxToStringFields)

    if (argumentString.nonEmpty) {
      s"""
         |$formattedNodeName
         |Arguments: $argumentString
         |""".stripMargin
    } else {
      s"""
         |$formattedNodeName
         |""".stripMargin
    }
  }

  protected def formattedNodeName: String = {
    val opId = getTagValue(QueryPlan.OP_ID_TAG).map(id => s"$id").getOrElse("unknown")
    val codegenId =
      getTagValue(QueryPlan.CODEGEN_ID_TAG).map(id => s" [codegen id : $id]").getOrElse("")
    s"($opId) $nodeName$codegenId"
  }

  /**
   * All the top-level subqueries of the current plan node. Nested subqueries are not included.
   */
  def subqueries: Seq[PlanType] = {
    expressions.flatMap(_.collect {
      case e: PlanExpression[_] => e.plan.asInstanceOf[PlanType]
    })
  }

  /**
   * All the subqueries of the current plan node and all its children. Nested subqueries are also
   * included.
   */
  def subqueriesAll: Seq[PlanType] = {
    val subqueries = this.flatMap(_.subqueries)
    subqueries ++ subqueries.flatMap(_.subqueriesAll)
  }

  /**
   * A variant of `collect`. This method not only apply the given function to all elements in this
   * plan, also considering all the plans in its (nested) subqueries
   */
  def collectWithSubqueries[B](f: PartialFunction[PlanType, B]): Seq[B] =
    (this +: subqueriesAll).flatMap(_.collect(f))

  override def innerChildren: Seq[QueryPlan[_]] = subqueries

  /**
   * A private mutable variable to indicate whether this plan is the result of canonicalization.
   * This is used solely for making sure we wouldn't execute a canonicalized plan.
   * See [[canonicalized]] on how this is set.
   */
  @transient private var _isCanonicalizedPlan: Boolean = false

  protected def isCanonicalizedPlan: Boolean = _isCanonicalizedPlan

  /**
   * Returns a plan where a best effort attempt has been made to transform `this` in a way
   * that preserves the result but removes cosmetic variations (case sensitivity, ordering for
   * commutative operations, expression id, etc.)
   *
   * Plans where `this.canonicalized == other.canonicalized` will always evaluate to the same
   * result.
   *
   * Plan nodes that require special canonicalization should override [[doCanonicalize()]].
   * They should remove expressions cosmetic variations themselves.
   */
  @transient final lazy val canonicalized: PlanType = {
    var plan = doCanonicalize()
    // If the plan has not been changed due to canonicalization, make a copy of it so we don't
    // mutate the original plan's _isCanonicalizedPlan flag.
    if (plan eq this) {
      plan = plan.makeCopy(plan.mapProductIterator(x => x.asInstanceOf[AnyRef]))
    }
    plan._isCanonicalizedPlan = true
    plan
  }

  /**
   * Defines how the canonicalization should work for the current plan.
   */
  protected def doCanonicalize(): PlanType = {
    val canonicalizedChildren = children.map(_.canonicalized)
    var id = -1
    mapExpressions {
      case a: Alias =>
        id += 1
        // As the root of the expression, Alias will always take an arbitrary exprId, we need to
        // normalize that for equality testing, by assigning expr id from 0 incrementally. The
        // alias name doesn't matter and should be erased.
        val normalizedChild = QueryPlan.normalizeExpressions(a.child, allAttributes)
        Alias(normalizedChild, "")(ExprId(id), a.qualifier)

      case ar: AttributeReference if allAttributes.indexOf(ar.exprId) == -1 =>
        // Top level `AttributeReference` may also be used for output like `Alias`, we should
        // normalize the epxrId too.
        id += 1
        ar.withExprId(ExprId(id)).canonicalized

      case other => QueryPlan.normalizeExpressions(other, allAttributes)
    }.withNewChildren(canonicalizedChildren)
  }

  /**
   * Returns true when the given query plan will return the same results as this query plan.
   *
   * Since its likely undecidable to generally determine if two given plans will produce the same
   * results, it is okay for this function to return false, even if the results are actually
   * the same.  Such behavior will not affect correctness, only the application of performance
   * enhancements like caching.  However, it is not acceptable to return true if the results could
   * possibly be different.
   *
   * This function performs a modified version of equality that is tolerant of cosmetic
   * differences like attribute naming and or expression id differences.
   */
  final def sameResult(other: PlanType): Boolean = this.canonicalized == other.canonicalized

  /**
   * Returns a `hashCode` for the calculation performed by this plan. Unlike the standard
   * `hashCode`, an attempt has been made to eliminate cosmetic differences.
   */
  final def semanticHash(): Int = canonicalized.hashCode()

  /**
   * All the attributes that are used for this plan.
   */
  lazy val allAttributes: AttributeSeq = children.flatMap(_.output)
}

object QueryPlan extends PredicateHelper {
  val OP_ID_TAG = TreeNodeTag[Int]("operatorId")
  val CODEGEN_ID_TAG = new TreeNodeTag[Int]("wholeStageCodegenId")

  /**
   * Normalize the exprIds in the given expression, by updating the exprId in `AttributeReference`
   * with its referenced ordinal from input attributes. It's similar to `BindReferences` but we
   * do not use `BindReferences` here as the plan may take the expression as a parameter with type
   * `Attribute`, and replace it with `BoundReference` will cause error.
   */
  def normalizeExpressions[T <: Expression](e: T, input: AttributeSeq): T = {
    e.transformUp {
      case s: PlanExpression[QueryPlan[_] @unchecked] =>
        // Normalize the outer references in the subquery plan.
        val normalizedPlan = s.plan.transformAllExpressions {
          case OuterReference(r) => OuterReference(QueryPlan.normalizeExpressions(r, input))
        }
        s.withNewPlan(normalizedPlan)

      case ar: AttributeReference =>
        val ordinal = input.indexOf(ar.exprId)
        if (ordinal == -1) {
          ar
        } else {
          ar.withExprId(ExprId(ordinal))
        }
    }.canonicalized.asInstanceOf[T]
  }

  /**
   * Composes the given predicates into a conjunctive predicate, which is normalized and reordered.
   * Then returns a new sequence of predicates by splitting the conjunctive predicate.
   */
  def normalizePredicates(predicates: Seq[Expression], output: AttributeSeq): Seq[Expression] = {
    if (predicates.nonEmpty) {
      val normalized = normalizeExpressions(predicates.reduce(And), output)
      splitConjunctivePredicates(normalized)
    } else {
      Nil
    }
  }

  /**
   * Converts the query plan to string and appends it via provided function.
   */
  def append[T <: QueryPlan[T]](
      plan: => QueryPlan[T],
      append: String => Unit,
      verbose: Boolean,
      addSuffix: Boolean,
      maxFields: Int = SQLConf.get.maxToStringFields,
      printOperatorId: Boolean = false): Unit = {
    try {
      plan.treeString(append, verbose, addSuffix, maxFields, printOperatorId)
    } catch {
      case e: AnalysisException => append(e.toString)
    }
  }
}

object QueryPlanIntegrity {

  private def canGetOutputAttrs[PlanType <: QueryPlan[PlanType]](
      p: PlanType,
      resolved: PlanType => Boolean): Boolean = {
    resolved(p) && !p.expressions.exists { e =>
      e.collectFirst {
        // We cannot call `output` in plans with a `ScalarSubquery` expr having no column,
        // so, we filter out them in advance.
        case s: ScalarSubquery if s.plan.schema.fields.isEmpty => true
      }.isDefined
    }
  }

  /**
   * Since some logical plans (e.g., `Union`) can build `AttributeReference`s in their `output`,
   * this method checks if the same `ExprId` refers to attributes having the same data type
   * in plan output.
   */
  def hasUniqueExprIdsForOutput[PlanType <: QueryPlan[PlanType]](
      plan: PlanType,
      resolved: PlanType => Boolean = (_: PlanType) => true,
      ignored: PlanType => Boolean = (_: PlanType) => false): Boolean = {
    val exprIds = plan.collect { case p if canGetOutputAttrs[PlanType](p, resolved) =>
      // NOTE: we still need to filter resolved expressions here because the output of
      // some resolved logical plans can have unresolved references,
      // e.g., outer references in `ExistenceJoin`.
      p.output.filter(_.resolved).map { a => (a.exprId, a.dataType) }
    }.flatten

    val ignoredExprIds = plan.collect {
      case p if ignored(p) => p.output.map(_.exprId)
    }.flatten.toSet

    val groupedDataTypesByExprId = exprIds.filterNot { case (exprId, _) =>
      ignoredExprIds.contains(exprId)
    }.groupBy(_._1).values.map(_.distinct)

    groupedDataTypesByExprId.forall(_.length == 1)
  }

  /**
   * This method checks if reference `ExprId`s are not reused when assigning a new `ExprId`.
   * For example, it returns false if plan transformers create an alias having the same `ExprId`
   * with one of reference attributes, e.g., `a#1 + 1 AS a#1`.
   */
  def checkIfSameExprIdNotReused[PlanType <: QueryPlan[PlanType]](
      plan: PlanType,
      resolved: PlanType => Boolean = (_: PlanType) => true): Boolean = {
    plan.collect { case p if resolved(p) =>
      p.expressions.forall {
        case a: Alias =>
          // Even if a plan is resolved, `a.references` can return unresolved references,
          // e.g., in `Grouping`/`GroupingID`, so we need to filter out them and
          // check if the same `exprId` in `Alias` does not exist
          // among reference `exprId`s.
          !a.references.filter(_.resolved).map(_.exprId).exists(_ == a.exprId)
        case _ =>
          true
      }
    }.forall(identity)
  }

  /**
   * This method checks if the same `ExprId` refers to an unique attribute in a plan tree.
   * Some plan transformers (e.g., `RemoveNoopOperators`) rewrite logical
   * plans based on this assumption.
   */
  def checkIfExprIdsAreGloballyUnique[PlanType <: QueryPlan[PlanType]](
      plan: PlanType,
      resolved: PlanType => Boolean = (_: PlanType) => true): Boolean = {
    checkIfSameExprIdNotReused[PlanType](plan, resolved) &&
      hasUniqueExprIdsForOutput[PlanType](plan, resolved)
  }
}
