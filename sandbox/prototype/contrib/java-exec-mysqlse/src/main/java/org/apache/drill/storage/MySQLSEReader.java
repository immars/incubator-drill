package org.apache.drill.storage;

import org.apache.drill.common.physical.MySQLScanReadEntry;
import org.apache.drill.exec.exception.ExecutionSetupException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OutputMutator;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.vector.ValueVector;
import org.apache.drill.exec.store.RecordReader;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

public class MySQLSEReader implements RecordReader {

  public static final int BATCH_SIZE = 8192;//batch size for record iteration 
  
  private FragmentContext context;
  private MySQLSEReadEntry entry;
  private BatchSchema expectedSchema;
  private OutputMutator output;
  
  private String sql;
  private ResultSet currentRS;
  
  private ValueVectorFactory vvf;
  private ArrayList<ValueVector> vectors;

  public MySQLSEReader(FragmentContext context, MySQLScanReadEntry myEntry) {
    this.entry = myEntry;
    this.context = context;
  }

  @Override
  public void setup(BatchSchema expectedSchema, OutputMutator output) throws ExecutionSetupException {
    try {
      this.expectedSchema = expectedSchema;
      this.output = output;
      //todo check schema compatibility, with MySQLScanPOP.output
      //build output
      vvf = new ValueVectorFactory(context.getAllocator());
      this.vectors= new ArrayList<>();
      for(MaterializedField field : expectedSchema){
        ValueVector v =  vvf.newVector(field);
        vectors.add(v);
        output.addField(v.getField().getFieldId(),v);
      }
      this.sql = buildSQL();
    } catch (SchemaChangeException e) {
      throw new ExecutionSetupException("Failure while setting up fields", e);
    }
  }

  private String buildSQL() {
    //TODO build sql string 
    return new SQLBuilder().buildSQL(this);
  }

  @Override
  public int next() throws Exception {
    if(currentRS == null){
      currentRS = MySQLQueryExecutor.getInstance().executeQuery(buildSQL());
    }
    ResultSetBuffer buffer = ResultSetBuffer.create(vectors,BATCH_SIZE);
    int i = 0;
    for(; i<BATCH_SIZE && currentRS.next();i++){
      buffer.sink(currentRS);
    }
    buffer.flush();
    return i;
  }

  @Override
  public void cleanup() {
    try {
      currentRS.close();
    } catch (SQLException e) {
      e.printStackTrace();  //e:
    }
    
  }

  public class SQLBuilder {
    public String buildSQL(MySQLSEReader reader) {
      return "";//TODO
    }
  }
}
