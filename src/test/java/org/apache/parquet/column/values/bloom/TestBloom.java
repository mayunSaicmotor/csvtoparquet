/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.parquet.column.values.bloom;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.parquet.io.api.Binary;
import org.junit.Test;


public class TestBloom {
  @Test
  public void testIntBloom () throws IOException {
    Bloom.IntBloom intBloom = new Bloom.IntBloom(269, Bloom.HASH.MURMUR3_X64_128, Bloom.ALGORITHM.BLOCK);
    for(int i = 0; i<10; i++) {
      intBloom.insert(i);
    }

    intBloom.flush();
    for(int i = 0; i<10; i++) {
      assertTrue(intBloom.find(i));
    }
  }

  @Test
  public void testBinaryBloom() throws IOException {
    Bloom.BinaryBloom binaryBloom = new Bloom.BinaryBloom(0, Bloom.HASH.MURMUR3_X64_128, Bloom.ALGORITHM.BLOCK);
    List<String> strings = new ArrayList<String>();
    RandomStr randomStr = new RandomStr();
    for(int i = 0; i<10000; i++) {
      String str = randomStr.get(10);
      strings.add(str);
      binaryBloom.insert(Binary.fromString(str));
    }

    binaryBloom.flush();
    for(int i = 0; i<strings.size(); i++) {
      assertTrue(binaryBloom.find(Binary.fromString(strings.get(i))));
    }

    // exist can be true in a very low probability.
    boolean exist = binaryBloom.find(Binary.fromString("not exist"));
    assertFalse(exist);
  }

  @Test
  public void testMurmur3() throws IOException {
    byte[] bytes = {0x12, 0x34, 0x56, 0x78};
    long hash = Murmur3.hash64(bytes);
    assertEquals(hash, -420773712042568267l);

  }
}
