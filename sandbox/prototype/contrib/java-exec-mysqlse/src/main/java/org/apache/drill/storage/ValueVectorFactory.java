package org.apache.drill.storage;

import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.vector.Int32Vector;
import org.apache.drill.exec.record.vector.ValueVector;

public class ValueVectorFactory {
  private final BufferAllocator allocator;
  private int nextID=0;

  public ValueVectorFactory(BufferAllocator allocator) {
    this.allocator = allocator;
  }

  public ValueVector newVector(MaterializedField field) {
    return new Int32Vector(nextID++, allocator);  //TODO method implementation
  }
}
