package org.apache.drill.storage;

import org.apache.drill.common.expression.types.DataType;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.vector.BitVector;
import org.apache.drill.exec.record.vector.Int32Vector;
import org.apache.drill.exec.record.vector.ValueVector;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ResultSetBuffer {

  private static final Map<DataType, Class> DataType2JavaType = new HashMap<>();
  
  static{
    DataType2JavaType.put(DataType.INT32, Integer.class);
    DataType2JavaType.put(DataType.INT64, Long.class);
    DataType2JavaType.put(DataType.BOOLEAN, Boolean.class);
    DataType2JavaType.put(DataType.BYTES, byte[].class);
  }
  
  private final List<ValueVector> vectors;
  private final int expectedSize;
  List<List<Object>> values;
  private int currentRow = 0;

  public ResultSetBuffer(List<ValueVector> vectors, int expectedSize) {
    this.vectors = vectors;
    this.expectedSize = expectedSize;
    reset();
  }

  public static ResultSetBuffer create(List<ValueVector> vectors, int expectedSize){
    return new ResultSetBuffer(vectors, expectedSize);
  }

  //todo ByteBuffer based sink
  public void sink(ResultSet resultSet) throws SQLException {
    for(int i=0;i<vectors.size();i++){
      ValueVector vector = vectors.get(i);
      values.get(i).add(resultSet.getObject(i, DataType2JavaType.get(vector.getField().getType())));
    }
    currentRow++;
  }

  public void reset(){
    currentRow=0;
    values = new ArrayList<>();
    for(ValueVector v:vectors){
      values.add(new ArrayList<>());
    }
  }

  /**
   * flushes data to underlying ValueVectors.
   * will reset inner buffer of ValueVectors.
   * @throws Exception
   */
  public void flush() throws Exception {
    for(ValueVector vector:vectors){
      vector.allocateNew(currentRow);
    }
    for(int row=0; row<currentRow;row++){
      for(int column=0;column<vectors.size();column++){
        ValueVector vector = vectors.get(column);
        Object value = values.get(column).get(row);
        if(vector instanceof Int32Vector){
          ((Int32Vector)vector).set(row, (Integer) value);
        }else if(vector instanceof BitVector){
          if((Boolean)value){((BitVector)vector).set(row);}
          else{((BitVector)vector).clear(row);}
        }//todo  else
      }
    }
  }
}
