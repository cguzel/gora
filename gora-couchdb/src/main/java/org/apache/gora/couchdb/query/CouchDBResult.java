package org.apache.gora.couchdb.query;

import org.apache.gora.couchdb.store.CouchDBStore;
import org.apache.gora.persistency.Persistent;
import org.apache.gora.query.Query;
import org.apache.gora.query.impl.ResultBase;
import org.apache.gora.query.ws.impl.ResultWSBase;
import org.apache.gora.store.DataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class CouchDBResult <K, T extends Persistent> extends ResultBase<K, T> {
  /**
   * Helper to write useful information into the logs
   */
  public static final Logger LOGGER = LoggerFactory.getLogger(CouchDBResult.class);

  private List<Map> result;

  protected CouchDBStore dataStore;
  int pos = 0;

  public CouchDBResult(DataStore<K, T> dataStore, Query<K, T> query, List<Map> result) {
    super(dataStore, query);
    this.result = result;
    this.dataStore= (CouchDBStore) dataStore;
  }

  @Override
  protected boolean nextInner() throws IOException {
    if (result == null || result.size() <= 0 || pos >= result.size()) {
      return false;
    }
    key = (K) result. get(pos++);
    persistent = (T) dataStore.newInstance(result.get(pos), query.getFields());
    return false;

  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return 0;
  }
}
