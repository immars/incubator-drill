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
import org.codehaus.commons.compiler.Location;
import org.codehaus.commons.compiler.jdk.ExpressionEvaluator;
import org.codehaus.janino.*;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.io.StringReader;
import java.lang.reflect.InvocationTargetException;
import java.util.List;

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
    DirectIntegerEvaluate eeDBody = compileDirectBody();
    benchmarkEvaluator(eeJanino); // warm up
    benchmarkDirectEvaluator(eeDirect); // warm up
    long janinoT = benchmarkEvaluator(eeJanino);
    long jdkT = benchmarkEvaluator(eeJDK);
    long directT = benchmarkDirectEvaluator(eeDirect);
    long dbodyT = benchmarkDirectEvaluator(eeDBody);
    
    System.out.println("Evaluate Janino: " + janinoT/1000 + " micros.  " +
      "JDK: " + jdkT/1000 + " micros. " +
      "Direct: " + directT/1000 + " micros. " +
      "DBody: " + dbodyT/1000 + " micros.");    

  }
  
  @Test
  public void compareCompiles() throws Exception{
    for (int i = 0; i < 100; i++) {
      compileJanino();
      compileJDK();
      compileDirect();
      compileDirectBody();
    }
    long janinoT = 0l, jdkT = 0l, directT = 0l, dbodyT = 0l;
    for (int i = 0; i < 100; i++) {
      long n1 = System.nanoTime();
      compileJanino();
      long n2 = System.nanoTime();
      compileJDK();
      long n3 = System.nanoTime();
      compileDirect();
      long n4 = System.nanoTime();
      compileDirectBody();
      long n5 = System.nanoTime();      
      janinoT = n2 - n1;
      jdkT = n3 - n2;
      directT = n4 - n3;
      dbodyT = n5 - n4;
      System.out.println("Compile Janino: " + janinoT/1000 + " micros.  " +
        "JDK: " + jdkT/1000 + " micros. " +
        "Direct: " + directT/1000 + " micros. " +
        "DBody: " + dbodyT/1000 + " micros.");    
    }
    
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
    SimpleCompilerEvaluatorBuilder<DirectIntegerEvaluate> eval = new SimpleCompilerEvaluatorBuilder<DirectIntegerEvaluate>(DirectIntegerEvaluate.class, "c > d ? c:d");
    return eval.getInstance();
  }
  
  private DirectIntegerEvaluate compileDirectBody() throws CompileException, IOException, ClassNotFoundException, InstantiationException, IllegalAccessException {
    IDirectEvaluatorBuilder<DirectIntegerEvaluate> eval = new ClassBodyEvaluatorBuilder("c > d ? c:d");
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
  
  public static interface IDirectEvaluatorBuilder<T> {
    T getInstance() throws IllegalAccessException, InstantiationException;
  }
  
  public static class SimpleCompilerEvaluatorBuilder<T> implements IDirectEvaluatorBuilder<T> {

    private final String eval;
    private final Class<T> evaluatorClass;
    private final ISimpleCompiler compiler;
    private final Class<T> implementorClass;
    public SimpleCompilerEvaluatorBuilder(Class<T> clz, String eval) throws IOException, CompileException, ClassNotFoundException {
      this.eval = eval;
      this.evaluatorClass = clz;
      compiler = new org.codehaus.janino.SimpleCompiler();
      String className = "Eval"+System.currentTimeMillis();
      compiler.cook(new StringReader("package my.pkg;\n" +
        "public class "+className+" implements org.apache.drill.exec.compile.TestClassCompilationTypes.DirectIntegerEvaluate{\n" +
        " public int evaluate(int c, int d){ return "+eval+";}}"));
      implementorClass = (Class<T>) compiler.getClassLoader().loadClass("my.pkg." + className);
      
    }
    
    @Override
    public T getInstance() throws IllegalAccessException, InstantiationException {
      return (T) implementorClass.newInstance();
    }
  }
  
  public static class ClassBodyEvaluatorBuilder implements IDirectEvaluatorBuilder<DirectIntegerEvaluate> {

    private final String eval;
    private final MySimpleCompiler compiler;
    private final Class<DirectIntegerEvaluate> implementorClass;
    public ClassBodyEvaluatorBuilder(String eval) throws IOException, CompileException, ClassNotFoundException {
      this.eval = eval;
      compiler = new MySimpleCompiler();
      String className = "Eval"+System.currentTimeMillis();      
      implementorClass = compiler.cook(eval, DirectIntegerEvaluate.class, className);
    }

    @Override
    public DirectIntegerEvaluate getInstance() throws IllegalAccessException, InstantiationException {
      return implementorClass.newInstance();
    }

  }
  
  public static class MySimpleCompiler extends ScriptEvaluator{
    public Class cook(String eval, Class clz, String className) throws CompileException, IOException {
      Java.CompilationUnit unit = new Java.CompilationUnit(null);
      this.setClassName(className);      
      this.setImplementedInterfaces(new Class[]{DirectIntegerEvaluate.class});

      this.setUpClassLoaders();
      Java.ClassDeclaration pmcd = this.addPackageMemberClassDeclaration(Location.NOWHERE, unit);
      
      List statements = this.makeStatements(0,new Scanner(null, new StringReader("return "+eval+";")));
      pmcd.addDeclaredMethod(this.makeMethodDeclaration(
        Location.NOWHERE,
        false,
        int.class,
        "evaluate",
        new Class[]{int.class, int.class},
        new String[]{"c", "d"},
        new Class[0],
        statements
      ));
      return this.compileToClass(unit, className);
    }
  }

  static class MyClassEvaluator extends ClassBodyEvaluator{
    public Class cook(Java.CompilationUnit unit, String newClassName) throws CompileException {
      return compileToClass(unit, newClassName);
    }
  }
  
}
