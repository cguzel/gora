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

import org.apache.gora.persistency.impl.PersistentBase;
import org.jdom.Element;
import org.jdom.input.SAXBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public class CouchDBMappingBuilder<K, T extends PersistentBase> {

  // Class description
  private static final String TAG_CLASS = "class";
  private static final String TAG_FIELD = "field";
  private static final String ATT_KEYCLASS = "keyClass";
  private static final String ATT_DOCUMENT = "document";

  // Document description
  private static final String ATT_NAME = "name";

  /**
   * Mapping instance being built
   */
  private final CouchDBMapping mapping;

  private final CouchDBStore<K, T> dataStore;

  public CouchDBMappingBuilder(final CouchDBStore<K, T> store) {
    this.dataStore = store;
    this.mapping = new CouchDBMapping();
  }

  /**
   * Return the built mapping if it is in a legal state
   */
  public CouchDBMapping build() {
    if (mapping.getDatabaseName() == null)
      throw new IllegalStateException("A collection is not specified");
    return mapping;
  }

  protected void readMapping(String filename) throws IOException {
    try {
      final Class<T> persistentClass = dataStore.getPersistentClass();
      final Class<K> keyClass = dataStore.getKeyClass();

      final SAXBuilder saxBuilder = new SAXBuilder();
      final InputStream is = getClass().getClassLoader().getResourceAsStream(filename);

      final Element root = saxBuilder.build(is).getRootElement();
      final List<Element> classElements = root.getChildren(TAG_CLASS);

      for (Element classElement : classElements) {
        if (haveKeyClass(keyClass, classElement) && havePersistentClass(persistentClass, classElement)) {
          mapping
              .setDatabaseName(dataStore.getSchemaName(classElement.getAttributeValue(ATT_DOCUMENT), persistentClass));
          mapping.fields = classElement.getChildren(TAG_FIELD);
          break;
        }
      }
    } catch (Exception ex) {
      CouchDBStore.LOG.error(ex.getMessage(), ex);
      throw new IOException(ex);
    }
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
}
