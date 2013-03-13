package org.apache.drill.adhoc;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.LogicalPlan;
import org.apache.drill.common.logical.OperatorGraph;
import org.apache.drill.common.logical.PlanProperties;
import org.apache.drill.common.logical.StorageEngineConfig;
import org.apache.drill.common.logical.data.LogicalOperator;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Select;
import org.apache.drill.exec.ref.IteratorRegistry;
import org.apache.drill.exec.ref.ReferenceInterpreter;
import org.apache.drill.exec.ref.RunOutcome;
import org.apache.drill.exec.ref.eval.BasicEvaluatorFactory;
import org.apache.drill.exec.ref.rse.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;


/**
 * Created with IntelliJ IDEA.
 * User: Wang Yufei
 * Date: 13-2-20
 * Time: 上午11:20
 * To change this template use File | Settings | File Templates.
 */
public class PlanParser {

    //static final Logger logger = LoggerFactory.getLogger(RunSimplePlan.class);
    private CCJSqlParserManager pm = new CCJSqlParserManager();
    private static PlanParser instance = new PlanParser();

    private PlanParser() {

    }

    public static PlanParser getInstance() {
        return instance;
    }

    public LogicalPlan parse(String sql) throws JSQLParserException, Exception {
        LogicalPlan plan = null;
        Statement statement = pm.parse(new StringReader(sql));
        if (statement instanceof Select) {
            Select selectStatement  = (Select) statement;
            AdhocSQLQueryVisitorImpl visitor = new AdhocSQLQueryVisitorImpl();
            selectStatement.getSelectBody().accept(visitor);
            List<LogicalOperator> logicalOperators = visitor.getLogicalOperators();

            ObjectMapper mapper = new ObjectMapper();
            PlanProperties head = mapper.readValue(new String("{\"type\":\"apache_drill_logical_plan\",\"version\":\"1\",\"generator\":{\"type\":\"manual\",\"info\":\"na\"}}").getBytes(), PlanProperties.class);

            //List<StorageEngineConfig> storageEngines = mapper.readValue(new String("[{\"type\":\"console\",\"name\":\"console\"},{\"type\":\"fs\",\"name\":\"fs\",\"root\":\"file:///\"}]").getBytes(),new TypeReference<List<StorageEngineConfig>>() {});
            List<StorageEngineConfig> storageEngines = new ArrayList<>();
            storageEngines.add(mapper.readValue(new String("{\"type\":\"hbase\",\"name\":\"hbase\"}").getBytes(),HBaseRSE.HBaseRSEConfig.class));
            storageEngines.add(mapper.readValue(new String("{\"type\":\"mysql\",\"name\":\"mysql\"}").getBytes(),MySQLRSE.MySQLRSEConfig.class));
            storageEngines.add(mapper.readValue(new String("{\"type\":\"console\",\"name\":\"console\"}").getBytes(),ConsoleRSE.ConsoleRSEConfig.class));
            storageEngines.add(mapper.readValue(new String("{\"type\":\"fs\",\"name\":\"fs\",\"root\":\"file:///\"}").getBytes(),FileSystemRSE.FileSystemRSEConfig.class));

            plan = new LogicalPlan(head, storageEngines, logicalOperators);
        }
        return plan;
    }

    public static void test(){
        SchemaPath schemaPath = new SchemaPath("sof-dsk_deu");


    }
    public static void main(String[] args) throws Exception{
        //logger.info("xxx");
        DrillConfig config = DrillConfig.create();
//        LogicalPlan logicalPlan = new PlanParser().parse("Select count(deu.uid) FROM (deu INNER JOIN deu ON fix_sof.uid=deu.uid) WHERE fix.register_time>=20130101000000 and fix.register_time<20130102000000 and deu.l0='visit' and deu.date='2013-01-02'\n");
//        logicalPlan.getGraph().getAdjList().printEdges();
//        IteratorRegistry ir = new IteratorRegistry();
//        ReferenceInterpreter i = new ReferenceInterpreter(logicalPlan, ir, new BasicEvaluatorFactory(ir), new RSERegistry(config));
//        System.out.println(logicalPlan.toJsonString(config));
//        i.setup();
//        Collection<RunOutcome> outcomes = i.run();

        //System.out.println(String.format("%1$s%1$s","a"));
        String sql= new String("Select count(deu.uid) FROM (fix INNER JOIN deu ON fix.uid=deu.uid) WHERE fix.register_time>=20130101000000 and fix.register_time<20130102000000 and deu.l0='visit' and deu.date='2013-01-02'").replace("-","xadrill");
        //String sql = new String("Select sof-dsk_deu.uid from sof-dsk_deu where sof-dsk_deu.date<'20130101' and sof-dsk_deu.date>'20130101' and sof-dsk_deu.l0='visit'").replace("-","xadrill");
        //String sql = "Select tencentxadrill18894_deu.uid from tencentxadrill18894_deu";
        LogicalPlan logicalPlan = new PlanParser().parse(sql);
        IteratorRegistry ir = new IteratorRegistry();
        ReferenceInterpreter i = new ReferenceInterpreter(logicalPlan, ir, new BasicEvaluatorFactory(ir), new RSERegistry(config));
        System.out.println(logicalPlan.toJsonString(config));
        i.setup();
        Collection<RunOutcome> outcomes = i.run();

    }
}
