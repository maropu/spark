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

import reflect.ClassTag

private[spark] class CudaResourceProvider extends ResourceRegister {

  override def createIterator[T: ClassTag, U: ClassTag](f: (Int, Iterator[T]) => Iterator[U])
    : (Int, Iterator[T]) => Iterator[U] = {
    // This function needs to convert a given `f` into driver CUDA code here.
    // We might use the same approach with SPARK-14083; it analyzes JVM bytecode and codegen
    // a CUDA driver function. However, this implementation seems complicated and
    // it is difficult to support an arbitrary user-provided function.
    throw new UnsupportedOperationException("Not supported yet")
  }

  override def getCapacity(): Int = {
    // Call a CUDA API (i.e., cudaGetDeviceCount) to get the number of GPUs implemented
    // in an executor by JNIs.
    1
  }

  override def shortName(): String = "cuda"
}
