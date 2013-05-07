package org.apache.drill.storage;

import java.sql.ResultSet;

public class MySQLQueryExecutor {

  private static MySQLQueryExecutor instance = new MySQLQueryExecutor();

  public static MySQLQueryExecutor getInstance() {
    return instance;
  }

  public ResultSet executeQuery(String sql) {
    return null;  //TODO method implementation
  }
}
