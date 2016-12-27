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

package org.apache.spark.executor

import java.util.ServiceLoader

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

import org.apache.spark.annotation.{Experimental, InterfaceStability}
import org.apache.spark.util.Utils

/**
 * ::Experimental::
 * Implemented by classes that can generate an iterator on a specific resource.
 *
 * @since 2.2.0
 */
@Experimental
@InterfaceStability.Unstable
trait ResourceRegister {

  // Converts a vanilla function into a resource-specific one
  def createIterator[T: ClassTag, U: ClassTag](f: (Int, Iterator[T]) => Iterator[U])
    : (Int, Iterator[T]) => Iterator[U]

  // Returns the number of this available resource
  def getCapacity(): Int

  def shortName(): String
}

private[spark] object ResourceUtils {

  private val resourcesArgString = """(.*):(\d+)""".r

  def parseResourcesString(argStr: String): Map[String, Int] = {
    argStr.split(",").flatMap {
      case resourcesArgString(resourceType, amount) => Some(resourceType -> amount.toInt)
      case _ => None
    }.toMap
  }

  def resourcesToString(resources: Map[String, Int]): String = {
    resources.map { case (resourceType, amount) => s"$resourceType:$amount" }.mkString(",")
  }

  def lookupResource(resourceType: String): ResourceRegister = {
    val loader = Utils.getContextOrSparkClassLoader
    val serviceLoader = ServiceLoader.load(classOf[ResourceRegister], loader)
    serviceLoader.asScala.filter(_.shortName().equalsIgnoreCase(resourceType)).toList match {
      case head :: Nil =>
        // there is exactly one registered alias
        head.getClass.newInstance()
      case sources =>
        // There are multiple registered aliases for the input resource type
        sys.error(s"Multiple resources found for $resourceType" +
          s"(${sources.map(_.getClass.getName).mkString(", ")}), " +
          "please specify the fully qualified class name.")
    }
  }

  private def getAllResources(): Map[String, Int] = {
    val loader = Utils.getContextOrSparkClassLoader
    val serviceLoader = ServiceLoader.load(classOf[ResourceRegister], loader)
    serviceLoader.asScala.map { provider =>
      provider.shortName -> provider.getCapacity
    }.toMap
  }

  def getAllResourceNames(): Set[String] = getAllResources().keySet

  def getAllAvailableResources(): Map[String, Int] = {
    getAllResources().filter { case (_, capacity) => capacity > 0 }
  }
}
