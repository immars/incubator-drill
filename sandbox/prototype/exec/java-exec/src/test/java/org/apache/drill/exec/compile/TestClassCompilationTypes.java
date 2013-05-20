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
package org.apache.drill.exec.compile;

import org.codehaus.commons.compiler.CompileException;
import org.codehaus.commons.compiler.IExpressionEvaluator;
import org.codehaus.commons.compiler.ISimpleCompiler;
import org.codehaus.commons.compiler.jdk.ExpressionEvaluator;
import org.codehaus.commons.compiler.jdk.SimpleCompiler;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.io.StringReader;
import java.lang.reflect.InvocationTargetException;

public class TestClassCompilationTypes {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestClassCompilationTypes.class);

  @Test @Ignore
  public void comparePerfs() throws Exception {
    for(int i =0; i < 500; i++){
      int r = 0;
      long n0 = System.nanoTime();
      r += janino();
      long n1 = System.nanoTime();
      r += jdk();
      long n2 = System.nanoTime();
      long janinoT = (n1 - n0)/1000;
      long jdkT = (n2 - n1)/1000;
      System.out.println("Janino: " + janinoT + "micros.  JDK: " + jdkT + "micros. Val" + r);
    }

  }
  
  @Test
  public void compareEvaluates() throws Exception {
    IExpressionEvaluator eeJanino = compileJanino();
    IExpressionEvaluator eeJDK = compileJDK();
    DirectIntegerEvaluate eeDirect = compileDirect();
    benchmarkEvaluator(eeJanino); // warm up
    benchmarkDirectEvaluator(eeDirect); // warm up
    long janinoT = benchmarkEvaluator(eeJanino)/1000;
    long jdkT = benchmarkEvaluator(eeJDK)/1000;
    long directT = benchmarkDirectEvaluator(eeDirect)/1000;
    
    System.out.println("Janino: " + janinoT + " micros.  JDK: " + jdkT + " micros. Direct: " + directT + " micros.");

  }
  private long benchmarkDirectEvaluator(DirectIntegerEvaluate ee) throws InvocationTargetException {
    long t = System.nanoTime();
    for(int i =0; i < 500; i++){
      ee.evaluate(10, 11);
    }
    return System.nanoTime() - t;    
  }

  private long benchmarkEvaluator(IExpressionEvaluator ee) throws InvocationTargetException {
    long t = System.nanoTime();
    for(int i =0; i < 500; i++){
      ee.evaluate(new Object[]{new Integer(10), new Integer(11),});
    }
    return System.nanoTime() - t;
  }

  IExpressionEvaluator compileJanino() throws Exception {
    // Compile the expression once; relatively slow.
    org.codehaus.janino.ExpressionEvaluator ee = new org.codehaus.janino.ExpressionEvaluator("c > d ? c : d", // expression
        int.class, // expressionType
        new String[] { "c", "d" }, // parameterNames
        new Class[] { int.class, int.class } // parameterTypes
    );
    return ee;
  }
  
  private IExpressionEvaluator compileJDK() throws CompileException {
    // Compile the expression once; relatively slow.
    ExpressionEvaluator ee = new ExpressionEvaluator("c > d ? c : d", // expression
        int.class, // expressionType
        new String[] { "c", "d" }, // parameterNames
        new Class[] { int.class, int.class } // parameterTypes
    );
    return ee;
  }
  
  private DirectIntegerEvaluate compileDirect() throws CompileException, IOException, ClassNotFoundException, InstantiationException, IllegalAccessException {
    DirectExpressionEvaluator<DirectIntegerEvaluate> eval = new DirectExpressionEvaluator<DirectIntegerEvaluate>(DirectIntegerEvaluate.class, "c > d ? c:d");
    return eval.getInstance();
  }
  
  private int janino() throws Exception{
    IExpressionEvaluator ee = compileJanino();
    // Evaluate it with varying parameter values; very fast.
    return (Integer) ee.evaluate(new Object[] { // parameterValues
        new Integer(10), new Integer(11), });
  }
  
    
  private int jdk() throws Exception{
    IExpressionEvaluator ee = compileJDK();
    // Evaluate it with varying parameter values; very fast.
    return  (Integer) ee.evaluate(new Object[] { // parameterValues
        new Integer(10), new Integer(11), });
  }

  
  public static interface DirectIntegerEvaluate{
    int evaluate(int c, int d);
  }
  
  public static class DirectExpressionEvaluator<T>{

    private final String eval;
    private final Class<T> evaluatorClass;
    private final ISimpleCompiler compiler;
    private final Class<T> implementorClass;
    public DirectExpressionEvaluator(Class<T> clz, String eval) throws IOException, CompileException, ClassNotFoundException {
      this.eval = eval;
      this.evaluatorClass = clz;
      compiler = new SimpleCompiler();
      String className = "Eval"+System.currentTimeMillis();
      compiler.cook(new StringReader("package my.pkg;\n" +
        "public class "+className+" implements org.apache.drill.exec.compile.TestClassCompilationTypes.DirectIntegerEvaluate{\n" +
        " public int evaluate(int c, int d){ return "+eval+";}}"));
      implementorClass = (Class<T>) compiler.getClassLoader().loadClass("my.pkg."+className);
      
    }
    
    public T getInstance() throws IllegalAccessException, InstantiationException {
      return (T) implementorClass.newInstance();
    }
  }
}
