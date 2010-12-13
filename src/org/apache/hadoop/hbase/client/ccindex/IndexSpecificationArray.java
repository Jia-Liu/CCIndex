/**
 * Copyright 2009 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.client.ccindex;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
/** Holds an array of index specifications.
 * 
 */
public class IndexSpecificationArray implements Writable {

  private IndexSpecification [] indexSpecifications;
  
  public IndexSpecificationArray() {
    // FOr writable
  }
  public IndexSpecificationArray(IndexSpecification[] specs) {
    this.indexSpecifications = specs;
  }
 
  public void readFields(DataInput in) throws IOException {
    int size = in.readInt();
    indexSpecifications = new IndexSpecification[size];
    for (int i=0; i<size; i++) {
      indexSpecifications[i] = new IndexSpecification();
      indexSpecifications[i].readFields(in);
    }

  }
  public void write(DataOutput out) throws IOException {
    out.writeInt(indexSpecifications.length);
    for (IndexSpecification indexSpec : indexSpecifications) {
      indexSpec.write(out);
    }
  }
  /** Get indexSpecifications.
   * @return indexSpecifications
   */
  public IndexSpecification[] getIndexSpecifications() {
    return indexSpecifications;
  }

}
