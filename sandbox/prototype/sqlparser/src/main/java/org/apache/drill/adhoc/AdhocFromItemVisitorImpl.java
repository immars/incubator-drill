package org.apache.drill.adhoc;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import org.apache.drill.common.expression.*;
import org.apache.drill.common.logical.JSONOptions;
import org.apache.drill.common.logical.data.*;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;

import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: Wang Yufei
 * Date: 13-2-20
 * Time: 上午11:45
 * To change this template use File | Settings | File Templates.
 */
public class AdhocFromItemVisitorImpl implements FromItemVisitor {

    private LogicalOperator lop;
    private LogicalExpression where;
    private String startDate;
    private String endDate;
    private String l0="*";
    private String l1="*";
    private String l2="*";
    private String l3="*";
    private String l4="*";


    public AdhocFromItemVisitorImpl(LogicalExpression where){
        super();
        this.where = where;
    }

    @Override
    public void visit(Table table) {
        String tableName = table.getName();
        FieldReference fr = new FieldReference(tableName);
        String storageEngine = null;
        JSONOptions selection =  null;
        ObjectMapper mapper = new ObjectMapper();
        try{
            if (tableName.contains("deu")){
                storageEngine = "hbase";
                getHBaseSelection(where);
                String _selection = String.format("{\"startDate\":\"%s\",\"endDate\":\"%s\", \"l0\":\"%s\",\"l1\":\"%s\",\"l2\":\"%s\",\"l3\":\"%s\",\"l4\":\"%s\"}",startDate,endDate,l0,l1,l2,l3,l4);
                selection = mapper.readValue(_selection.getBytes(), JSONOptions.class);
            }else{
                storageEngine="mysql";
                selection = mapper.readValue(new String("{\"sql\":\"None\"}").getBytes(), JSONOptions.class);
            }
        }catch (Exception e){
            e.printStackTrace();
            System.out.println(e.getMessage());
        }

        lop = new Scan(storageEngine,selection,fr);

    }

    @Override
    public void visit(SubSelect subSelect) {
        AdhocSQLQueryVisitorImpl visitor = new AdhocSQLQueryVisitorImpl();
        subSelect.getSelectBody().accept(visitor);
        List<LogicalOperator> lops = visitor.getLogicalOperators();
        for (LogicalOperator lopTmp : lops) {
            System.out.println(lopTmp);
        }
    }

    @Override
    public void visit(SubJoin subJoin) {
        FromItem left = subJoin.getLeft();
        FromItem right = subJoin.getJoin().getRightItem();
        AdhocFromItemVisitorImpl lv = new AdhocFromItemVisitorImpl(where);
        left.accept(lv);
        AdhocFromItemVisitorImpl rv = new AdhocFromItemVisitorImpl(where);
        right.accept(rv);

        LogicalOperator leftLop = lv.getLogicalOperator();
        LogicalOperator rightLop = rv.getLogicalOperator();

        Expression expr = subJoin.getJoin().getOnExpression();
        JoinCondition[] jcs = null;
        if (expr != null) {
            AdhocExpressionVisitorImpl exprVisitor = new AdhocExpressionVisitorImpl();
            expr.accept(exprVisitor);
            FunctionCall funcExpr = (FunctionCall) exprVisitor.getLogicalExpression();
            List<LogicalExpression> args = funcExpr.args;
            String relationship = getRelationship(funcExpr.getDefinition().getName());
            JoinCondition jc = new JoinCondition(relationship, args.get(0), args.get(1));
            jcs = new JoinCondition[1];
            jcs[0] = jc;
        } else {
            jcs = new JoinCondition[0];
        }
        lop = new Join(leftLop, rightLop, jcs, returnJoinType(subJoin));
    }

    public LogicalOperator getLogicalOperator() {
        return lop;
    }

    private String returnJoinType(SubJoin subJoin) {
        if (subJoin.getJoin().isInner()) {
            return "INNER";
        } else if (subJoin.getJoin().isNatural()) {
            return "OUTER";
        } else if (subJoin.getJoin().isOuter()) {
            return "OUTER";
        } else if (subJoin.getJoin().isLeft()) {
            return "LEFT";
        }
        //throw new AdhocRuntimeException("Can't parse join type of " + subJoin.toString());
        //throw new Exception(); //wcl
        return "";
    }

    private String getRelationship(String name) {
        if (name.equals("equal")) {
            return "==";
        }
        //throw new AdhocRuntimeException("Can't parse join condition relationship " + name);
        return "";
    }

    private void getHBaseSelection(LogicalExpression where){
        if(where instanceof FunctionCall){
            ImmutableList<LogicalExpression> args = ((FunctionCall)where).args;
            FunctionDefinition functionDefinition =  ((FunctionCall)where).getDefinition();
            if(! (args.get(0) instanceof FunctionCall) && !(args.get(1) instanceof FunctionCall)){
                if(args.get(0) instanceof SchemaPath){
                    if(args.get(1) instanceof ValueExpressions.QuotedString){
                        hbaseHelper((SchemaPath)args.get(0),(ValueExpressions.QuotedString)args.get(1), functionDefinition);
                    }
                }else{
                    if(args.get(1) instanceof ValueExpressions.QuotedString){
                        hbaseHelper((SchemaPath)args.get(1),(ValueExpressions.QuotedString)args.get(0), functionDefinition);
                    }
                }
            }
            getHBaseSelection(args.get(0));
            getHBaseSelection(args.get(1));
        }
    }

    private void hbaseHelper(SchemaPath schemaPath, ValueExpressions.QuotedString quotedString, FunctionDefinition functionDefinition){
        String path = schemaPath.getPath().toString();
        String value = quotedString.value;
        String funcionName = functionDefinition.getName();
        if (path.contains("deu.date")){
            if (funcionName.equals("less than")){
                endDate = value;
            }else if (funcionName.equals("greater than")){
                startDate = value;
            }else{
                endDate=startDate=value;
            }
        }

        if(path.contains("deu.l0")){
            l0 = value;
        }
        if(path.contains("deu.l1")){
            l1 = value;
        }
        if(path.contains("deu.l2")){
            l2 = value;
        }
        if(path.contains("deu.l3")){
            l3 = value;
        }
        if(path.contains("deu.l4")){
            l4 = value;
        }
    }
    private String getMySQLSelection(LogicalExpression where){
        return null;
    }
}
