package org.apache.gora.couchdb.store;

import org.jdom.Element;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CouchDBMapping {

  public String databaseName;
  public List<Element> fields;

  public String getDatabaseName() {
    return databaseName;
  }

  public void setDatabaseName(String databaseName) {
    this.databaseName = databaseName;
  }


}
