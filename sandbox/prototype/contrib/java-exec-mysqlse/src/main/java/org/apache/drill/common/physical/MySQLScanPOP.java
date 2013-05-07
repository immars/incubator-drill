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
package org.apache.drill.common.physical;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.common.physical.FieldSet;
import org.apache.drill.common.physical.pop.base.AbstractScan;
import org.apache.drill.common.physical.pop.base.PhysicalOperator;
import org.apache.drill.common.physical.pop.base.Scan;
import org.apache.drill.common.proto.CoordinationProtos;

import java.util.List;

@JsonTypeName("scan-mysql")
public class MySQLScanPOP extends AbstractScan<MySQLScanReadEntry> {
  
  @JsonProperty("ops")
  public List<PhysicalOperator> pushedDowns;
  
  @JsonProperty("output")
  public FieldSet output;

  @Override
  public void applyAssignments(List<CoordinationProtos.DrillbitEndpoint> endpoints) {
    //TODO method implementation
  }

  @Override
  public Scan<?> getSpecificScan(int minorFragmentId) {
    return null;  //TODO method implementation
  }

  @Override
  public List<EndpointAffinity> getOperatorAffinity() {
    return null;  //TODO method implementation
  }
}
