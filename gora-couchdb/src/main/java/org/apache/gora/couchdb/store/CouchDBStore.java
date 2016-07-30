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

import com.google.common.primitives.Ints;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.util.Utf8;
import org.apache.commons.lang.StringUtils;
import org.apache.gora.couchdb.query.CouchDBQuery;
import org.apache.gora.couchdb.query.CouchDBResult;
import org.apache.gora.couchdb.util.GoraStdObjectMapperFactory;
import org.apache.gora.persistency.Persistent;
import org.apache.gora.persistency.impl.PersistentBase;
import org.apache.gora.query.PartitionQuery;
import org.apache.gora.query.Query;
import org.apache.gora.query.Result;
import org.apache.gora.query.impl.PartitionQueryImpl;
import org.apache.gora.store.DataStoreFactory;
import org.apache.gora.store.impl.DataStoreBase;
import org.apache.gora.util.AvroUtils;
import org.apache.gora.util.IOUtils;
import org.ektorp.CouchDbConnector;
import org.ektorp.CouchDbInstance;
import org.ektorp.ViewQuery;
import org.ektorp.http.HttpClient;
import org.ektorp.http.StdHttpClient;
import org.ektorp.impl.ObjectMapperFactory;
import org.ektorp.impl.StdCouchDbConnector;
import org.ektorp.impl.StdCouchDbInstance;
import org.ektorp.support.CouchDbDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Implementation of a CouchDB data store to be used by gora.
 *
 * @param <K> class to be used for the key
 * @param <T> class to be persisted within the store
 */
public class CouchDBStore<K, T extends PersistentBase> extends DataStoreBase<K, T> {

  /**
   * Logging implementation
   */
  protected static final Logger LOG = LoggerFactory.getLogger(CouchDBStore.class);

  /**
   * The default file name value to be used for obtaining the CouchDB object field mapping's
   */
  public static final String DEFAULT_MAPPING_FILE = "gora-couchdb-mapping.xml";

  public static final ConcurrentHashMap<Schema, SpecificDatumReader<?>> readerMap = new ConcurrentHashMap<>();
  public static final ConcurrentHashMap<Schema, SpecificDatumWriter<?>> writerMap = new ConcurrentHashMap<>();

  /**
   * Default schema index with value "0" used when AVRO Union data types are stored
   */
  public static final int DEFAULT_UNION_SCHEMA = 0;

  /**
   * Mapping definition for CouchDB
   */
  private CouchDBMapping mapping;

  /**
   * The standard implementation of the CouchDbInstance interface. This interface provides methods for
   * managing databases on the connected CouchDb instance.
   * StdCouchDbInstance is thread-safe.
   */
  private CouchDbInstance dbInstance;

  /**
   * The standard implementation of the CouchDbConnector interface. This interface provides methods for
   * manipulating documents within a specific database.
   * StdCouchDbConnector is thread-safe.
   */
  private CouchDbConnector db;

  /**
   * Initialize the data store by reading the credentials, setting the client's properties up and
   * reading the mapping file. Initialize is called when then the call to
   * {@link org.apache.gora.store.DataStoreFactory#createDataStore} is made.
   *
   * @param keyClass
   * @param persistentClass
   * @param properties
   */
  @Override
  public void initialize(Class<K> keyClass, Class<T> persistentClass, Properties properties) {
    LOG.debug("Initializing CouchDB store");
    super.initialize(keyClass, persistentClass, properties);

    final CouchDBParameters params = CouchDBParameters.load(properties);

    try {
      final String mappingFile = DataStoreFactory.getMappingFile(properties, this, DEFAULT_MAPPING_FILE);
      final HttpClient httpClient = new StdHttpClient.Builder()
          .url("http://" + params.getServer() + ":" + params.getPort())
          //          .username(params.getUser())
          //          .password(params.getPass())
          .build();

      dbInstance = new StdCouchDbInstance(httpClient);

      final CouchDBMappingBuilder<K, T> builder = new CouchDBMappingBuilder<>(this);
      LOG.debug("Initializing CouchDB store with mapping {}.", new Object[] { mappingFile });
      builder.readMapping(mappingFile);
      mapping = builder.build();

      final ObjectMapperFactory myObjectMapperFactory = new GoraStdObjectMapperFactory();
      myObjectMapperFactory.createObjectMapper().addMixInAnnotations(persistentClass, CouchDbDocument.class);

      db = new StdCouchDbConnector(mapping.getDatabaseName(), dbInstance, myObjectMapperFactory);
      db.createDatabaseIfNotExists();

    } catch (IOException e) {
      LOG.error("Error while initializing CouchDB store: {}", new Object[] { e.getMessage() });
      throw new RuntimeException(e);
    }
  }

  /**
   * In CouchDB, Schemas are referred to as database name.
   *
   * @return databasename
   */
  @Override
  public String getSchemaName() {
    return mapping.getDatabaseName();
  }

  /**
   * Create a new database in CouchDB if necessary.
   */
  @Override
  public void createSchema() {
    if (schemaExists()) {
      return;
    }
    dbInstance.createDatabase(mapping.getDatabaseName());
  }

  /**
   * Drop the database.
   */
  @Override
  public void deleteSchema() {
    if (schemaExists()) {
      dbInstance.deleteDatabase(mapping.getDatabaseName());
    }
  }

  /**
   * Check if the database already exists or should be created.
   */
  @Override
  public boolean schemaExists() {
    return dbInstance.checkIfDbExists(mapping.getDatabaseName());
  }

  /**
   * Retrieve an entry from the store with only selected fields.
   *
   * @param key    identifier of the document in the database
   * @param fields list of fields to be loaded from the database
   */
  @Override
  public T get(final K key, final String[] fields) {

    final Map result = db.get(Map.class, key.toString());

    try {
      return newInstance(result, getFieldsToQuery(fields));
    } catch (IOException e) {
      LOG.error(e.getMessage(), e);
      return null;
    }
  }

  /**
   * Persist an object into the store.
   *
   * @param key identifier of the object in the store
   * @param obj the object to be inserted
   */
  @Override
  public void put(K key, T obj) {

    final Map<String, Object> doc = new HashMap<>();
    doc.put("_id", key.toString());

    Schema schema = obj.getSchema();

    List<Field> fields = schema.getFields();
    for (int i = 0; i < fields.size(); i++) {
      if (!obj.isDirty(i)) {
        continue;
      }
      Field field = fields.get(i);
      Schema.Type type = field.schema().getType();
      Object fieldValue = obj.get(field.pos());
      Schema fieldSchema = field.schema();
      // check if field has a nested structure (array, map, record or union)
      fieldValue = serializeFieldValue(fieldSchema, fieldValue);
      doc.put(field.name(), fieldValue);
    }
    db.update(doc);

  }

  private Object serializeFieldValue(Schema fieldSchema, Object fieldValue) {
    switch (fieldSchema.getType()) {

    case MAP:
    case ARRAY:
    case RECORD:
      byte[] data = null;
      try {
        @SuppressWarnings("rawtypes")
        SpecificDatumWriter writer = getDatumWriter(fieldSchema);
        data = IOUtils.serialize(writer, fieldValue);
      } catch (IOException e) {
        LOG.error(e.getMessage(), e);
      }
      fieldValue = data;
      break;
    case BYTES:
      fieldValue = ((ByteBuffer) fieldValue).array();
      break;
    case ENUM:
    case STRING:
      fieldValue = fieldValue.toString();
      break;
    case UNION:
      // If field's schema is null and one type, we do undertake serialization.
      // All other types are serialized.
      if (fieldSchema.getTypes().size() == 2 && isNullable(fieldSchema)) {
        int schemaPos = getUnionSchema(fieldValue, fieldSchema);
        Schema unionSchema = fieldSchema.getTypes().get(schemaPos);
        fieldValue = serializeFieldValue(unionSchema, fieldValue);
      } else {
        byte[] serilazeData = null;
        try {
          @SuppressWarnings("rawtypes")
          SpecificDatumWriter writer = getDatumWriter(fieldSchema);
          serilazeData = IOUtils.serialize(writer, fieldValue);
        } catch (IOException e) {
          LOG.error(e.getMessage(), e);
        }
        fieldValue = serilazeData;
      }
      break;
    default:
      break;
    }
    return fieldValue;
  }

  private SpecificDatumWriter getDatumWriter(Schema fieldSchema) {
    SpecificDatumWriter writer = writerMap.get(fieldSchema);
    if (writer == null) {
      writer = new SpecificDatumWriter(fieldSchema);// ignore dirty bits
      writerMap.put(fieldSchema, writer);
    }

    return writer;
  }

  private int getUnionSchema(Object pValue, Schema pUnionSchema) {
    int unionSchemaPos = 0;
    //    String valueType = pValue.getClass().getSimpleName();
    for (Schema currentSchema : pUnionSchema.getTypes()) {
      Schema.Type schemaType = currentSchema.getType();
      if (pValue instanceof CharSequence && schemaType.equals(Schema.Type.STRING))
        return unionSchemaPos;
      else if (pValue instanceof ByteBuffer && schemaType.equals(Schema.Type.BYTES))
        return unionSchemaPos;
      else if (pValue instanceof Integer && schemaType.equals(Schema.Type.INT))
        return unionSchemaPos;
      else if (pValue instanceof Long && schemaType.equals(Schema.Type.LONG))
        return unionSchemaPos;
      else if (pValue instanceof Double && schemaType.equals(Schema.Type.DOUBLE))
        return unionSchemaPos;
      else if (pValue instanceof Float && schemaType.equals(Schema.Type.FLOAT))
        return unionSchemaPos;
      else if (pValue instanceof Boolean && schemaType.equals(Schema.Type.BOOLEAN))
        return unionSchemaPos;
      else if (pValue instanceof Map && schemaType.equals(Schema.Type.MAP))
        return unionSchemaPos;
      else if (pValue instanceof List && schemaType.equals(Schema.Type.ARRAY))
        return unionSchemaPos;
      else if (pValue instanceof Persistent && schemaType.equals(Schema.Type.RECORD))
        return unionSchemaPos;
      unionSchemaPos++;
    }
    // if we weren't able to determine which data type it is, then we return the default
    return DEFAULT_UNION_SCHEMA;
  }

  /**
   * Deletes the object with the given key
   *
   * @param key the key of the object
   * @return whether the object was successfully deleted
   */
  @Override
  public boolean delete(K key) {
    final String keyString = key.toString();
    final Map<String, Object> referenceData = db.get(Map.class, keyString);
    return StringUtils.isNotEmpty(db.delete(keyString, referenceData.get("_rev").toString()));
  }

  /**
   * Deletes all the objects matching the query.
   * See also the note on <a href="#visibility">visibility</a>.
   *
   * @param query matching records to this query will be deleted
   * @return number of deleted records
   */
  @Override
  public long deleteByQuery(Query<K, T> query) {
    return delete(query.getKey()) ? 1 : 0;
  }

  /**
   * Create a new {@link Query} to query the datastore.
   */
  @Override
  public Query<K, T> newQuery() {
    CouchDBQuery<K, T> query = new CouchDBQuery<>(this);
    query.setFields(getFieldsToQuery(null));
    return query;
  }

  /**
   * Execute the query and return the result.
   */
  @Override
  public Result<K, T> execute(Query<K, T> query) {
    query.setFields(getFieldsToQuery(query.getFields()));
    final ViewQuery viewQuery = new ViewQuery()
        .allDocs()
        .includeDocs(true)
        .limit(Ints.checkedCast(query.getLimit())); //FIXME GORA have long value but ektorp client use integer

    CouchDBResult<K, T> couchDBResult = new CouchDBResult<>(this, query, db.queryView(viewQuery, Map.class));

    return couchDBResult;
  }

  @Override
  public List<PartitionQuery<K, T>> getPartitions(Query<K, T> query)
      throws IOException {
    final List<PartitionQuery<K, T>> list = new ArrayList<>();
    final PartitionQueryImpl<K, T> pqi = new PartitionQueryImpl<>(query);
    pqi.setConf(getConf());
    list.add(pqi);
    return list;
  }

  /**
   * Creates a new Persistent instance with the values in 'result' for the fields listed.
   *
   * @param result result from the query to the database
   * @param fields the list of fields to be mapped to the persistence class instance
   * @return a persistence class instance which content was deserialized
   * @throws IOException
   */
  public T newInstance(Map<String, Object> result, String[] fields)
      throws IOException {
    if (result == null)
      return null;

    T persistent = newPersistent();

    for (String f : result.keySet()) {
      if (f.equals("_id") || f.equals("_rev")) {
        continue;
      }
      final Field field = fieldMap.get(f);
      final Schema fieldSchema = field.schema();
      final Object resultObj = deserializeFieldValue(field, fieldSchema, result.get(field.name()), persistent);
      persistent.put(field.pos(), resultObj);
      persistent.setDirty(field.pos());
    }

    persistent.clearDirty();
    return persistent;

  }

  private Object deserializeFieldValue(Field field, Schema fieldSchema, Object value, T persistent) throws IOException {
    Object fieldValue = null;
    switch (fieldSchema.getType()) {
    case MAP:
    case ARRAY:
    case RECORD:
      SpecificDatumReader reader = getDatumReader(fieldSchema);
      fieldValue = IOUtils.deserialize((byte[]) value, reader, persistent.get(field.pos()));
      break;
    case ENUM:
      fieldValue = AvroUtils.getEnumValue(fieldSchema, (String) value);
      break;
    case BYTES:
      ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
      fieldValue = ByteBuffer.wrap(byteOut.toByteArray());
      break;
    case STRING:
      fieldValue = new Utf8(value.toString());
      break;
    case LONG:
      fieldValue = Long.valueOf(value.toString());
      break;
    case INT:
      fieldValue = Integer.valueOf(value.toString());
      break;
    case DOUBLE:
      fieldValue = Double.valueOf(value.toString());
      break;
    case UNION:
      if (fieldSchema.getTypes().size() == 2 && isNullable(fieldSchema)) {
        Schema.Type type0 = fieldSchema.getTypes().get(0).getType();
        Schema.Type type1 = fieldSchema.getTypes().get(1).getType();

        // Check if types are different and there's a "null", like
        // ["null","type"] or ["type","null"]
        if (!type0.equals(type1)) {
          if (type0.equals(Schema.Type.NULL))
            fieldSchema = fieldSchema.getTypes().get(1);
          else
            fieldSchema = fieldSchema.getTypes().get(0);
        } else {
          fieldSchema = fieldSchema.getTypes().get(0);
        }
        fieldValue = deserializeFieldValue(field, fieldSchema, value,
            persistent);
      } else {
        SpecificDatumReader unionReader = getDatumReader(fieldSchema);
        fieldValue = IOUtils.deserialize((byte[]) value, unionReader, persistent.get(field.pos()));
        break;
      }
      break;
    default:
      fieldValue = value;
    }
    return fieldValue;
  }

  private SpecificDatumReader getDatumReader(Schema fieldSchema) {
    SpecificDatumReader<?> reader = readerMap.get(fieldSchema);
    if (reader == null) {
      reader = new SpecificDatumReader(fieldSchema);// ignore dirty bits
      final SpecificDatumReader localReader = readerMap.putIfAbsent(fieldSchema, reader);
      if (localReader != null) {
        reader = localReader;
      }
    }
    return reader;
  }

  private boolean isNullable(Schema unionSchema) {
    for (Schema innerSchema : unionSchema.getTypes()) {
      if (innerSchema.getType().equals(Schema.Type.NULL)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public void flush() {
    db.flushBulkBuffer();     //FIXME is true?
  }

  @Override
  public void close() {
  }
}