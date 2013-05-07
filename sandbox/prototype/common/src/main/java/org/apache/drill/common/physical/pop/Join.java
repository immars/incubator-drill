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
package org.apache.drill.common.physical.pop;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.Iterators;
import org.apache.drill.common.logical.data.JoinCondition;
import org.apache.drill.common.physical.pop.base.AbstractBase;
import org.apache.drill.common.physical.pop.base.PhysicalOperator;
import org.apache.drill.common.physical.pop.base.PhysicalVisitor;

import java.util.Iterator;

@JsonTypeName("join")
public class Join extends AbstractBase {
  
  public PhysicalOperator left;
  public PhysicalOperator right;
  private final org.apache.drill.common.logical.data.Join.JoinType type;
  private final JoinCondition[] conditions;
  
  
  @JsonCreator
  public Join(@JsonProperty("left") PhysicalOperator left, @JsonProperty("right") PhysicalOperator right, @JsonProperty("conditions") JoinCondition[] conditions, @JsonProperty("type") String type) {
    super();
    this.conditions = conditions;
    this.left = left;
    this.right = right;
    this.type = org.apache.drill.common.logical.data.Join.JoinType.resolve(type);
  }
  
  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
    return physicalVisitor.visitUnknown(this, value);    
  }

  @Override
  public Iterator<PhysicalOperator> iterator() {
    return Iterators.forArray(left, right);
  }
}
