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

package org.apache.spark.sql.catalyst.expressions.codegen;

import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.array.ByteArrayMethods;

public abstract class GrowableBuffer {

  private static final int ARRAY_MAX = ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH;

  protected byte[] buffer;
  protected int cursor = Platform.BYTE_ARRAY_OFFSET;

  GrowableBuffer() {
    this.buffer = new byte[16];
  }

  GrowableBuffer(int initialSize) {
    this.buffer = new byte[initialSize];
  }

  // Grows the buffer by at least `neededSize`
  public boolean grow(int neededSize) {
    if (neededSize > ARRAY_MAX - totalSize()) {
      throw new UnsupportedOperationException(
        "Cannot grow internal buffer by size " + neededSize + " because the size after growing " +
          "exceeds size limitation " + ARRAY_MAX);
    }
    final int length = totalSize() + neededSize;
    if (buffer.length < length) {
      int newLength = length < ARRAY_MAX / 2 ? length * 2 : ARRAY_MAX;
      final byte[] tmp = new byte[newLength];
      Platform.copyMemory(
        buffer,
        Platform.BYTE_ARRAY_OFFSET,
        tmp,
        Platform.BYTE_ARRAY_OFFSET,
        totalSize());
      buffer = tmp;
      return true;
    }
    return false;
  }

  public final int totalSize() {
    return cursor - Platform.BYTE_ARRAY_OFFSET;
  }
}
