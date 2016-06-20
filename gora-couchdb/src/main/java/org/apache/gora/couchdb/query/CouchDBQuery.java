package org.apache.gora.couchdb.query;

import org.apache.gora.couchdb.store.CouchDBMapping;
import org.apache.gora.couchdb.store.CouchDBStore;
import org.apache.gora.persistency.impl.PersistentBase;
import org.apache.gora.query.impl.QueryBase;
import org.apache.gora.store.DataStore;
import org.ektorp.ViewQuery;

public class CouchDBQuery<K,T extends PersistentBase> extends
    QueryBase<K,T> {

  final CouchDBStore store;

  public CouchDBQuery() {
    super(null);
    store = null;
  }

  public CouchDBQuery(DataStore<K, T> dataStore) {
    super(dataStore);
    store = (CouchDBStore) dataStore;
  }
}
