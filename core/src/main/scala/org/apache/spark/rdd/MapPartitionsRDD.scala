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

package org.apache.spark.rdd

import scala.reflect.ClassTag

import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.executor.ResourceUtils
import org.apache.spark.scheduler.cluster.ExecutorInfo._

/**
 * An RDD that applies the provided function to every partition of the parent RDD.
 */
private[spark] class MapPartitionsRDD[U: ClassTag, T: ClassTag](
    var prev: RDD[T],
    var f: (Int, Iterator[T]) => Iterator[U],
    val resourceType: Option[String] = Some(defaultResourceType),
    preservesPartitioning: Boolean = false)
  extends RDD[U](prev) {

  override val partitioner = if (preservesPartitioning) firstParent[T].partitioner else None

  override def getPartitions: Array[Partition] = firstParent[T].partitions

  private def computeOnResource(resourceType: String, split: Partition, context: TaskContext)
    : Iterator[U] = try {
    val func = ResourceUtils.lookupResource(resourceType).createIterator(f)
    func(split.index, firstParent[T].iterator(split, context))
  } catch {
    case t: Throwable =>
      logWarning(s"Failed to execute RDD#${this.id} on a resource '${resourceType}' and " +
        s"fell back into a regular path because: ${t.getMessage}")
      f(split.index, firstParent[T].iterator(split, context))
  }

  override def compute(split: Partition, context: TaskContext): Iterator[U] = {
    resourceType.find(_ != defaultResourceType).map { rType =>
      computeOnResource(rType, split, context)
    }.getOrElse {
      f(split.index, firstParent[T].iterator(split, context))
    }
  }

  override def clearDependencies() {
    super.clearDependencies()
    prev = null
  }
}
