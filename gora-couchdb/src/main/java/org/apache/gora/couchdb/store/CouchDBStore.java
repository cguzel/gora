/**
    * Licensed to the Apache Software Foundation (ASF) under one
    * or more contributor license agreements.  See the NOTICE file
    * distributed with this work for additional information
    * regarding copyright ownership.  The ASF licenses this file
    * to you under the Apache License, Version 2.0 (the
    * "License"); you may not use this file except in compliance
    * with the License.  You may obtain a copy of the License at
    *
    *     http://www.apache.org/licenses/LICENSE-2.0
    *
    * Unless required by applicable law or agreed to in writing, software
    * distributed under the License is distributed on an "AS IS" BASIS,
    * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    * See the License for the specific language governing permissions and
    * limitations under the License.
    */

package org.apache.gora.couchdb.store;

import org.apache.gora.persistency.impl.PersistentBase;
import org.apache.gora.query.PartitionQuery;
import org.apache.gora.query.Query;
import org.apache.gora.query.Result;
import org.apache.gora.store.DataStoreFactory;
import org.apache.gora.store.impl.DataStoreBase;
import org.ektorp.CouchDbConnector;
import org.ektorp.CouchDbInstance;
import org.ektorp.http.HttpClient;
import org.ektorp.http.StdHttpClient;
import org.ektorp.impl.StdCouchDbInstance;
import org.jdom.Element;
import org.jdom.input.SAXBuilder;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.util.List;
import java.util.Properties;

public class CouchDBStore<K,T extends PersistentBase> extends DataStoreBase<K,T> {

  private static final String DEFAULT_MAPPING_FILE = "gora-couchdb-mapping.xml";
  private static final String TAG_CLASS = "class";
  private static final String ATT_NAME = "name";
  private static final String ATT_TYPE = "type";
  private static final String ATT_KEYCLASS = "keyClass";
  private static final String ATT_DOCUMENT = "document";

  private CouchDBMapping mapping;
  private CouchDbInstance dbInstance;
  private CouchDbConnector db; //FIXME burada connection kapatman lazım

  @Override
  public void initialize(Class<K> keyClass, Class<T> persistentClass, Properties properties) {
    LOG.debug("Initializing CouchDB store");
      super.initialize(keyClass, persistentClass, properties);

    HttpClient httpClient = null;
    try {
      httpClient = new StdHttpClient.Builder()
          .url("http://localhost:5984")
          .build();
    } catch (MalformedURLException e) {
      e.printStackTrace();
    }
    dbInstance = new StdCouchDbInstance(httpClient);

    try {

      String mappingFile = DataStoreFactory
          .getMappingFile(properties, this, DEFAULT_MAPPING_FILE);
      mapping = readMapping(mappingFile);
    } catch (IOException e) {
      e.printStackTrace();
    }

  }

  protected CouchDBMapping readMapping(String filename) throws IOException {
    try {
      SAXBuilder saxBuilder = new SAXBuilder();
      InputStream is = getClass().getResourceAsStream(filename);
      if (is == null) {
        String msg = "Unable to load the mapping from resource '" + filename
            + "' as it does not appear to exist! " + "Trying local file.";
        LOG.warn(msg);
        is = new FileInputStream(filename);
      }

      final Element root = saxBuilder.build(is).getRootElement();
      final List<Element> classElements = root.getChildren(TAG_CLASS);
      for (Element classElement : classElements) {
        if (haveKeyClass(keyClass, classElement)
            && havePersistentClass(persistentClass, classElement)) {
          return getSchemaName(classElement.getAttributeValue(ATT_DOCUMENT), persistentClass)
        }
      }
    } catch (Exception ex) {
      LOG.error(ex.getMessage());
      LOG.error(ex.getStackTrace().toString());
      throw new IOException(ex);
    }
    throw new IOException("Unable to load the mapping from resource"); //FIXME
  }


  private boolean havePersistentClass(final Class<T> persistentClass,
      final Element classElement) {
    return classElement.getAttributeValue(ATT_NAME).equals(
        persistentClass.getName());
  }

  private boolean haveKeyClass(final Class<K> keyClass,
      final Element classElement) {
    return classElement.getAttributeValue(ATT_KEYCLASS).equals(
        keyClass.getName());
  }

  @Override
  public String getSchemaName() {
    return mapping.getDatabaseName();
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
    if(schemaExists()){
      dbInstance.deleteDatabase(mapping.getDatabaseName());
    }
  }

  @Override
  public boolean schemaExists() {
    return dbInstance.checkIfDbExists(mapping.getDatabaseName());
  }

  @Override
  public T get(final K key, final String[] fields) {
//    String[] dbFields = getFieldsToQuery(fields);
//
//    for (String f : fields) {
//      HBaseColumn col = mapping.getColumn(f);
//      if (col == null) {
//        throw new  RuntimeException("CouchDB mapping for field ["+ f +"] not found. " +
//            "Wrong gora-couchdb-mapping.xml?");
//      }
//      Schema fieldSchema = fieldMap.get(f).schema();
//      addFamilyOrColumn(get, col, fieldSchema);
//    }
//

    return null;
  }

  @Override
  public void put(K key, T obj) {
    db.upda
  }

  @Override
  public boolean delete(K key) {
    return false;
  }

  @Override
  public long deleteByQuery(Query<K, T> query) {
    return 0;
  }

  @Override
  public Result<K, T> execute(Query<K, T> query) {
    return null;
  }

  @Override
  public Query<K, T> newQuery() {
    return null;
  }

  @Override
  public List<PartitionQuery<K, T>> getPartitions(Query<K, T> query)
      throws IOException {
    return null;
  }

  @Override
  public void flush() {

  }

  @Override
  public void close() {

  }
}