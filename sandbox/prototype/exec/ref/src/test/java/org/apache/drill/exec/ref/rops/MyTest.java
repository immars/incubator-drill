package org.apache.drill.exec.ref.rops;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.logical.LogicalPlan;
import org.apache.drill.common.util.FileUtils;
import org.apache.drill.exec.ref.IteratorRegistry;
import org.apache.drill.exec.ref.ReferenceInterpreter;
import org.apache.drill.exec.ref.RunOutcome;
import org.apache.drill.exec.ref.eval.BasicEvaluatorFactory;
import org.apache.drill.exec.ref.rse.RSERegistry;
import org.junit.Test;
import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Collection;

/**
 * Author: mulisen Date: 3/18/13
 */
public class MyTest {

  @Test
  public void hello() throws NullPointerException {
    System.out.println("Hello");
  }

  @Test
  public void testJoin() throws Exception{
    DrillConfig config = DrillConfig.create();
    LogicalPlan plan = LogicalPlan.parse(config, Files.toString(FileUtils.getResourceAsFile("/my_join.json"), Charsets.UTF_8));
    IteratorRegistry ir = new IteratorRegistry();
    ReferenceInterpreter i = new ReferenceInterpreter(plan, ir, new BasicEvaluatorFactory(ir), new RSERegistry(config));

    PrintStream standardOutputStream = System.out;
    ByteArrayOutputStream redirectedOutput = new ByteArrayOutputStream();
    System.setOut(new PrintStream(redirectedOutput));
    i.setup();
    Collection<RunOutcome> outcomes = i.run();
    redirectedOutput.write(new String("end output").getBytes());

    String expectResult =
    "Breaking because no more records were found.\n"+
    "{\n"+
    "  \"departments\" : {\n"+
    "    \"name\" : \"Sales\",\n"+
    "    \"deptId\" : 31\n"+
    "  },\n"+
    "  \"employees\" : {\n"+
    "    \"lastName\" : \"Rafferty\",\n"+
    "    \"deptId\" : 31\n"+
    "  }\n"+
    "} \n"+
    " {\n"+
    "  \"departments\" : {\n"+
    "    \"name\" : \"Engineering\",\n"+
    "    \"deptId\" : 33\n"+
    "  },\n"+
    "  \"employees\" : {\n"+
    "    \"lastName\" : \"Jones\",\n"+
    "    \"deptId\" : 33\n"+
    "  }\n"+
    "} \n"+
    "end output";

    String output = redirectedOutput.toString();
    assertTrue(output.contains(expectResult));

    System.setOut(standardOutputStream);
  }
}
