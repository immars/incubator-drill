package org.apache.drill.common.physical;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.common.physical.pop.base.AbstractStore;
import org.apache.drill.common.physical.pop.base.PhysicalOperator;
import org.apache.drill.common.physical.pop.base.Store;
import org.apache.drill.common.proto.CoordinationProtos;

import java.util.Collections;
import java.util.List;


@JsonTypeName("log-store")
public class LogStorePOP extends AbstractStore {
  
  @JsonCreator
  public LogStorePOP(@JsonProperty("child") PhysicalOperator child) {
    super(child);
  }

  @Override
  public void applyAssignments(List<CoordinationProtos.DrillbitEndpoint> endpoints) {
    
  }

  @Override
  public Store getSpecificStore(PhysicalOperator child, int minorFragmentId) {
    return this;
  }

  @Override
  public int getMaxWidth() {
    return 1;
  }

  @Override
  public List<EndpointAffinity> getOperatorAffinity() {
    return Collections.emptyList();
  }
}
