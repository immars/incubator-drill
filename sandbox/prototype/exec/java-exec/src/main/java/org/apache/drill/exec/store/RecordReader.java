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
package org.apache.drill.exec.store;

import org.apache.drill.exec.exception.ExecutionSetupException;
import org.apache.drill.exec.ops.OutputMutator;
import org.apache.drill.exec.record.BatchSchema;

import java.sql.SQLException;

public interface RecordReader {

  /**
   * Configure the RecordReader with the provided schema and the record batch that should be written to.
   * 
   * @param knownSchema
   *          The set of fields that should be written to as well as the expected types for those fields. In the case
   *          that RecordReader has a known schema and the expectedSchema does not match the actual schema, a
   *          ExceptionSetupException will be thrown.
   * @param output
   *          The place where output for a particular scan should be written. The record reader is responsible for
   *          mutating the set of schema values for that particular record.
   * @throws ExecutionSetupException
   */
  public abstract void setup(BatchSchema expectedSchema, OutputMutator output) throws ExecutionSetupException;

  /**
   * Increment record reader forward, writing into the provided output batch.  
   * 
   * @return The number of additional records added to the output.
   */
  public abstract int next() throws Exception;

  public abstract void cleanup();

}