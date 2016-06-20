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

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.util.Utf8;
import org.apache.commons.lang.StringUtils;
import org.apache.gora.couchdb.query.CouchDBQuery;
import org.apache.gora.couchdb.query.CouchDBResult;
import org.apache.gora.persistency.impl.PersistentBase;
import org.apache.gora.query.PartitionQuery;
import org.apache.gora.query.Query;
import org.apache.gora.query.Result;
import org.apache.gora.query.impl.PartitionQueryImpl;
import org.apache.gora.store.DataStoreFactory;
import org.apache.gora.store.impl.DataStoreBase;
import org.apache.gora.util.AvroUtils;
import org.apache.gora.util.IOUtils;
import org.apache.gora.util.OperationNotSupportedException;
import org.ektorp.CouchDbConnector;
import org.ektorp.CouchDbInstance;
import org.ektorp.UpdateConflictException;
import org.ektorp.ViewQuery;
import org.ektorp.http.HttpClient;
import org.ektorp.http.StdHttpClient;
import org.ektorp.impl.StdCouchDbInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class CouchDBStore<K, T extends PersistentBase> extends DataStoreBase<K, T> {

  protected static final Logger LOG = LoggerFactory.getLogger(CouchDBStore.class);

  private static final String DEFAULT_MAPPING_FILE = "gora-couchdb-mapping.xml";
  private static final ConcurrentHashMap<Schema, SpecificDatumReader<?>> readerMap = new ConcurrentHashMap<>();

  private CouchDBMapping mapping;
  private CouchDbInstance dbInstance;
  private CouchDbConnector db; //TODO close connection

  @Override
  public void initialize(Class<K> keyClass, Class<T> persistentClass, Properties properties) {
    LOG.debug("Initializing CouchDB store");
    super.initialize(keyClass, persistentClass, properties);

    try {
      final String mappingFile = DataStoreFactory.getMappingFile(properties, this, DEFAULT_MAPPING_FILE);
      final HttpClient httpClient = new StdHttpClient.Builder()
          .url(properties.getProperty("url"))
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
    query.setFields(getFieldsToQuery(query.getFields()));
    final ViewQuery viewQuery = new ViewQuery()
        .allDocs()
        .includeDocs(true)
        .limit(new Long(query.getLimit()).intValue()); //FIXME GORA have long value but ektorp client use integer

    CouchDBResult<K,T> couchDBResult = new CouchDBResult<>(this, query, db.queryView(viewQuery,Map.class));

    return couchDBResult;
  }

  @Override
  public Query<K, T> newQuery() {
    CouchDBQuery<K, T> query = new CouchDBQuery<>(this);
    query.setFields(getFieldsToQuery(null));
    return query;
  }

  @Override
  public List<PartitionQuery<K, T>> getPartitions(Query<K, T> query)
      throws IOException {
    List<PartitionQuery<K, T>> list = new ArrayList<>();
    PartitionQueryImpl<K, T> pqi = new PartitionQueryImpl<>(query);
    pqi.setConf(getConf());
    list.add(pqi);
    return list;
  }

  public CouchDBMapping getMapping() {
    return mapping;
  }

  public T newInstance(Map result, String[] fields)
      throws IOException {
    if(result == null)
      return null;

    T persistent = newPersistent();

    if (fields == null) {
      fields = fieldMap.keySet().toArray(new String[fieldMap.size()]);     // FIXME Is this necessary? Think
    }

    for (String f : fields) {

      Field field = fieldMap.get(f);
      persistent.put(field.pos(), result.get(field.name()));
      persistent.setDirty(field.pos());
    }

    persistent.clearDirty();
    return persistent;

  }

//  @SuppressWarnings("unchecked")
//  private Object deserializeFieldValue(Field field, Schema fieldSchema, Object value, T persistent) throws IOException {
//    Object fieldValue = null;
//    switch (fieldSchema.getType()) {
//    case MAP:
//    case ARRAY:
//    case RECORD:
//      @SuppressWarnings("rawtypes")
//      SpecificDatumReader reader = getDatumReader(fieldSchema);
//      fieldValue = IOUtils.deserialize((byte[]) value, reader, persistent.get(field.pos()));
//      break;
//    case ENUM:
//      fieldValue = AvroUtils.getEnumValue(fieldSchema, (String) value);
//      break;
//    case FIXED:
//      throw new IOException("???");
//    case BYTES:
//      fieldValue = ByteBuffer.wrap((byte[]) value);
//      break;
//    case STRING:
//      fieldValue = new Utf8(value.toString());
//      break;
//    case UNION:
//      if (fieldSchema.getTypes().size() == 2 && isNullable(fieldSchema)) {
//        Schema.Type type0 = fieldSchema.getTypes().get(0).getType();
//        Schema.Type type1 = fieldSchema.getTypes().get(1).getType();
//
//        // Check if types are different and there's a "null", like
//        // ["null","type"] or ["type","null"]
//        if (!type0.equals(type1)) {
//          if (type0.equals(Schema.Type.NULL))
//            fieldSchema = fieldSchema.getTypes().get(1);
//          else
//            fieldSchema = fieldSchema.getTypes().get(0);
//        } else {
//          fieldSchema = fieldSchema.getTypes().get(0);
//        }
//        fieldValue = deserializeFieldValue(field, fieldSchema, value,
//            persistent);
//      } else {
//        @SuppressWarnings("rawtypes")
//        SpecificDatumReader unionReader = getDatumReader(fieldSchema);
//        fieldValue = IOUtils.deserialize((byte[]) value, unionReader, persistent.get(field.pos()));
//        break;
//      }
//      break;
//    default:
//      fieldValue = value;
//    }
//    return fieldValue;
//  }
//
//  private SpecificDatumReader getDatumReader(Schema fieldSchema) {
//    SpecificDatumReader<?> reader = readerMap.get(fieldSchema);
//    if (reader == null) {
//      reader = new SpecificDatumReader(fieldSchema);// ignore dirty bits
//      SpecificDatumReader localReader = null;
//      if ((localReader = readerMap.putIfAbsent(fieldSchema, reader)) != null) {
//        reader = localReader;
//      }
//    }
//    return reader;
//  }
//
//  private boolean isNullable(Schema unionSchema) {
//    for (Schema innerSchema : unionSchema.getTypes()) {
//      if (innerSchema.getType().equals(Schema.Type.NULL)) {
//        return true;
//      }
//    }
//    return false;
//  }

  @Override
  public void flush() {
    db.flushBulkBuffer();     //FIXME is true?
  }

  @Override
  public void close() {
  }
}