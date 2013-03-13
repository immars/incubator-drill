package org.apache.drill.exec.ref.rse;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.logical.StorageEngineConfigBase;
import org.apache.drill.common.logical.data.Scan;
import org.apache.drill.exec.ref.rops.ROP;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

/**
 * Created with IntelliJ IDEA.
 * User: hadoop
 * Date: 3/11/13
 * Time: 6:04 AM
 * To change this template use File | Settings | File Templates.
 */
public class MySQLRSE extends RSEBase {

    public MySQLRSE(MySQLRSEConfig engineConfig, DrillConfig config){

    }

    @JsonTypeName("mysql")
    public static class MySQLRSEConfig extends StorageEngineConfigBase {
        @JsonCreator
        public MySQLRSEConfig(@JsonProperty("name") String name) {
            super(name);
        }
    }

    public static class MySQLInputConfig implements ReadEntry{
        public String startKey;
        public String endKey;
    }

    @Override
    public Collection<ReadEntry> getReadEntries(Scan scan) throws IOException {
        MySQLInputConfig mySQLInputConfig = scan.getSelection().getWith(MySQLInputConfig.class);
        return Collections.singleton((ReadEntry) mySQLInputConfig);
    }

    @Override
    public RecordReader getReader(ReadEntry readEntry, ROP parentROP) throws IOException {
        MySQLInputConfig e = getReadEntry(MySQLInputConfig.class, readEntry);
        return  null;//wcl return mysqlrecordreader
    }
}
