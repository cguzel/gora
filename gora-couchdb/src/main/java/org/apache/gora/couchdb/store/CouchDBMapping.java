package org.apache.gora.couchdb.store;

import java.util.HashMap;
import java.util.Map;

public class CouchDBMapping {

  String databaseName;
  private final Map<String, String> columnMap = new HashMap<>();



  public String getDatabaseName() {
    return databaseName;
  }

  public void setDatabaseName(String databaseName) {
    this.databaseName = databaseName;
  }

  public void addColumn(String key, String name) {
    columnMap.put(key,name);
  }

}
