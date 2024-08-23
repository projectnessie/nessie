/*
 * Copyright (C) 2022 Dremio
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
package org.projectnessie.versioned.storage.dynamodb;

final class DynamoDBConstants {

  // This is the hard item size limit in DynamoDB
  static final int ITEM_SIZE_LIMIT = 400 * 1024;
  static final int BATCH_GET_LIMIT = 100;
  static final int BATCH_WRITE_MAX_REQUESTS = 25;

  static final String TABLE_REFS = "refs";
  static final String TABLE_OBJS = "objs";

  static final String COL_REFERENCES_POINTER = "p";
  static final String COL_REFERENCES_DELETED = "d";
  static final String COL_REFERENCES_CREATED_AT = "c";
  static final String COL_REFERENCES_EXTENDED_INFO = "e";
  static final String COL_REFERENCES_PREVIOUS = "h";

  static final String COL_REFERENCES_CONDITION_COMMON =
      "(" + COL_REFERENCES_DELETED + " = :deleted) AND (" + COL_REFERENCES_POINTER + " = :pointer)";

  static final String KEY_NAME = "k";

  static final String COL_OBJ_TYPE = "y";
  static final String COL_OBJ_VERS = "V";
  static final String COL_OBJ_REFERENCED = "z";

  static final String CONDITION_STORE_REF = "attribute_not_exists(" + COL_REFERENCES_POINTER + ")";
  static final String CONDITION_STORE_OBJ = "attribute_not_exists(" + COL_OBJ_TYPE + ")";

  private DynamoDBConstants() {}
}
