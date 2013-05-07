/*******************************************************************************
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package org.apache.drill.storage;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.drill.common.physical.MySQLScanPOP;
import org.apache.drill.exec.store.StorageEngine;

import java.util.List;

public class MySQLSEReadEntry implements StorageEngine.ReadEntry {
  
  public int id;
  public String location;
  public List<String> tables;  
  public MySQLScanPOP parent;

  @JsonCreator
  public MySQLSEReadEntry(@JsonProperty("id") int id,@JsonProperty("location") String location,@JsonProperty("tables") List<String> tables,@JsonProperty("parent") MySQLScanPOP parent) {
    this.id = id;
    this.location = location;
    this.tables = tables;
    this.parent = parent;
  }

  @Override
  public StorageEngine.Cost getCostEstimate() {
    return null;  //TODO method implementation
  }
}
