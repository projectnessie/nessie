/*
 * Copyright (C) 2020 Dremio
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dremio.nessie.versioned.store.mongodb;

/**
 * Set of constants for the MongoDb versioned store.
 */
public class MongoDbConstants {
  // The names of the collections in the database. These relate to
  // {@link com.dremio.nessie.versioned.store.ValueType}
  public static final String L1_COLLECTION = "nessie_l1";
  public static final String L2_COLLECTION = "nessie_l2";
  public static final String L3_COLLECTION = "nessie_l3";
  public static final String REF_COLLECTION = "nessie_ref";
  public static final String VALUE_COLLECTION = "nessie_value";
  public static final String KEY_FRAGMENT_COLLECTION = "nessie_key_fragment";
  public static final String COMMIT_METADATA_COLLECTION = "nessie_commit_metadata";

  public static final String STRING_PREFIX = "s";
  public static final String NUMBER_PREFIX = "n";
}
