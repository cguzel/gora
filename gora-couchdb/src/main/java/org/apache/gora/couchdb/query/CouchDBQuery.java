package org.apache.gora.couchdb.query;

import org.apache.gora.persistency.impl.PersistentBase;
import org.apache.gora.query.impl.QueryBase;
import org.apache.gora.store.DataStore;

public class CouchDBQuery<K,T extends PersistentBase> extends
    QueryBase<K,T> {

  public CouchDBQuery() {
    super(null);
  }

  public CouchDBQuery(DataStore<K, T> dataStore) {
    super(dataStore);
  }
}
