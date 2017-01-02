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

  override def createIterator[T: ClassTag, U: ClassTag](code: String, inputIter: Iterator[T])
    : Iterator[U] = {
    // This function needs to compile a given `code` for GPUs by using a CUDA runtime compilation
    // library (See: http://docs.nvidia.com/cuda/nvrtc/index.html#axzz4UcdlabgP).
    // In this design, I assume that some parts of operations in DataFrame (e.g., sorting and
    // aggregates) are implicitly pushed down into GPUs; we prepare CUDA code templates
    // for these operations, generate specific code for input queries, and pass the code into
    // `RDD#mapPartitionsWithResource` so as to get the iterator that uses GPUs internally.
    throw new UnsupportedOperationException("Not supported yet")
  }

  override def getCapacity(): Int = {
    // Call a CUDA API (i.e., cudaGetDeviceCount) to get the number of GPUs implemented
    // in an executor by JNIs.
    1
  }

  override def shortName(): String = "cuda"
}
