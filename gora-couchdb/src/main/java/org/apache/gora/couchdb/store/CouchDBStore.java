/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gora.couchdb.store;

import org.apache.avro.Schema.Field;
import org.apache.commons.lang.StringUtils;
import org.apache.gora.persistency.impl.PersistentBase;
import org.apache.gora.query.PartitionQuery;
import org.apache.gora.query.Query;
import org.apache.gora.query.Result;
import org.apache.gora.store.DataStoreFactory;
import org.apache.gora.store.impl.DataStoreBase;
import org.apache.gora.util.OperationNotSupportedException;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;
import org.ektorp.CouchDbConnector;
import org.ektorp.CouchDbInstance;
import org.ektorp.UpdateConflictException;
import org.ektorp.http.HttpClient;
import org.ektorp.http.StdHttpClient;
import org.ektorp.impl.StdCouchDbInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class CouchDBStore<K, T extends PersistentBase> extends DataStoreBase<K, T> {

  protected static final Logger LOG = LoggerFactory.getLogger(CouchDBStore.class);

  private static final String DEFAULT_MAPPING_FILE = "gora-couchdb-mapping.xml";

  private CouchDBMapping mapping;
  private CouchDbInstance dbInstance;
  private CouchDbConnector db; //FIXME burada connection kapatman lazım

  @Override
  public void initialize(Class<K> keyClass, Class<T> persistentClass, Properties properties) {
    LOG.debug("Initializing CouchDB store");
    super.initialize(keyClass, persistentClass, properties);

    try {
      final String mappingFile = DataStoreFactory.getMappingFile(properties, this, DEFAULT_MAPPING_FILE);
      final HttpClient httpClient = new StdHttpClient.Builder()
          .url("http://localhost:5984")
          .build();
      dbInstance = new StdCouchDbInstance(httpClient);

      CouchDBMappingBuilder<K, T> builder = new CouchDBMappingBuilder<>(this);
      LOG.debug("Initializing CouchDB store with mapping {}.", new Object[] { mappingFile });
      builder.readMapping(mappingFile);
      mapping = builder.build();

      db = dbInstance.createConnector(mapping.getDatabaseName(), true);
      db.createDatabaseIfNotExists();
    } catch (IOException e) {
      LOG.error("Error while initializing CouchDB store: {}", new Object[] { e.getMessage() });
      throw new RuntimeException(e);
    }
  }

  @Override
  public String getSchemaName() {
    return mapping.getDatabaseName();
  }

  @Override
  public String getSchemaName(final String mappingSchemaName, final Class<?> persistentClass) {
    return super.getSchemaName(mappingSchemaName, persistentClass);
  }

  @Override
  public void createSchema() {
    if (schemaExists()) {
      return;
    }
    dbInstance.createDatabase(mapping.getDatabaseName());
  }

  @Override
  public void deleteSchema() {
    if (schemaExists()) {
      dbInstance.deleteDatabase(mapping.getDatabaseName());
    }
  }

  @Override
  public boolean schemaExists() {
    return dbInstance.checkIfDbExists(mapping.getDatabaseName());
  }

  @Override
  public T get(final K key, final String[] fields) {
    return (T) db.get(Map.class, key.toString());
  }

  @Override
  public void put(K key, T obj) {

    final Map<String, Object> doc = new HashMap<>();
    doc.put("_id", key.toString());

    if (obj.isDirty()) {
      for (Field f : obj.getSchema().getFields()) {
        if (obj.isDirty(f.pos()) && (obj.get(f.pos()) != null)) {
          Object value = obj.get(f.pos());
          doc.put(f.name(), value.toString());
        }
        try {
          db.update(doc);
        } catch(UpdateConflictException e) {
          Map<String, Object> referenceData = db.get(Map.class, key.toString());
          db.delete(key.toString(), referenceData.get("_rev").toString());
          db.update(doc);
        }
      }
    } else {
      LOG.info("Ignored putting object {} in the store as it is neither new, neither dirty.", new Object[] { obj });
    }
  }
//@Override
//  public void put(K key, T obj) {
//
//    final Map<String, Object> doc = new HashMap<>();
//    JsonNode json = db.get(JsonNode.class, key.toString());
//
//    if (obj.isDirty()) {
//      for (Field f : obj.getSchema().getFields()) {
//        if (obj.isDirty(f.pos()) && (obj.get(f.pos()) != null)) {
//          Object value = doc.get(f.pos());
//
//          JsonNode field = json.findPath(f.name());
//          if (field.isObject()) {
//            ObjectNode a = (ObjectNode) field;
//            a.put(f.name(), value.toString());
//          }
//        }
//        db.update(json);
//      }
//    } else {
//      LOG.info("Ignored putting object {} in the store as it is neither new, neither dirty.", new Object[] { obj });
//    }
//  }

  @Override
  public boolean delete(K key) {
    return StringUtils.isNotEmpty(db.delete(key));
  }

  @Override
  public long deleteByQuery(Query<K, T> query) {
    throw new OperationNotSupportedException("delete is not supported for CoucchDBStore"); //FIXME create couchdbQuery
  }

  @Override
  public Result<K, T> execute(Query<K, T> query) {
    throw new OperationNotSupportedException(
        "execute is not supported for CoucchDBStore"); //FIXME create couchdbQuery
  }

  @Override
  public Query<K, T> newQuery() {
    throw new OperationNotSupportedException(
        "newQuery is not supported for CoucchDBStore"); //FIXME create couchdbQuery

  }

  @Override
  public List<PartitionQuery<K, T>> getPartitions(Query<K, T> query)
      throws IOException {
    throw new OperationNotSupportedException(
        "execute is not supported for CoucchDBStore"); //FIXME create couchdbQuery

  }

  @Override
  public void flush() {
    db.flushBulkBuffer();     //FIXME is true?
  }

  @Override
  public void close() {
  }
}