package org.apache.gora.couchdb.query;

import org.apache.gora.persistency.Persistent;
import org.apache.gora.query.Query;
import org.apache.gora.query.impl.ResultBase;
import org.apache.gora.query.ws.impl.ResultWSBase;
import org.apache.gora.store.DataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class CouchDBResult <K, T extends Persistent> extends ResultBase<K, T> {
  /**
   * Helper to write useful information into the logs
   */
  public static final Logger LOGGER = LoggerFactory.getLogger(CouchDBResult.class);

  public CouchDBResult(DataStore<K, T> dataStore,
      Query<K, T> query) {
    super(dataStore, query);
  }

  @Override
  protected boolean nextInner() throws IOException {
    return false;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return 0;
  }
}
