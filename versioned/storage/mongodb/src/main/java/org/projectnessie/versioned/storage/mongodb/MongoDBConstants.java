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
package org.projectnessie.versioned.storage.mongodb;

final class MongoDBConstants {

  static final String TABLE_REFS = "refs";
  static final String TABLE_OBJS = "objs";

  static final String ID_PROPERTY_NAME = "_id";

  static final String COL_REFERENCES_NAME = "n";
  static final String COL_REFERENCES_POINTER = "p";
  static final String COL_REFERENCES_DELETED = "d";
  static final String COL_REFERENCES_CREATED_AT = "c";
  static final String COL_REFERENCES_EXTENDED_INFO = "e";
  static final String COL_REFERENCES_PREVIOUS = "h";

  static final String COL_OBJ_ID = "i";
  static final String COL_REPO = "r";
  static final String COL_OBJ_TYPE = "y";

  static final String COL_COMMIT = "c";
  static final String COL_REF = "e";
  static final String COL_VALUE = "v";
  static final String COL_SEGMENTS = "I";
  static final String COL_INDEX = "i";
  static final String COL_TAG = "t";
  static final String COL_STRING = "s";

  static final String COL_COMMIT_CREATED = "c";
  static final String COL_COMMIT_SEQ = "q";
  static final String COL_COMMIT_MESSAGE = "m";
  static final String COL_COMMIT_HEADERS = "h";
  static final String COL_COMMIT_REFERENCE_INDEX = "x";
  static final String COL_COMMIT_REFERENCE_INDEX_STRIPES = "r";
  static final String COL_COMMIT_TAIL = "t";
  static final String COL_COMMIT_SECONDARY_PARENTS = "s";
  static final String COL_COMMIT_INCREMENTAL_INDEX = "i";
  static final String COL_COMMIT_INCOMPLETE_INDEX = "n";
  static final String COL_COMMIT_TYPE = "y";

  static final String COL_REF_NAME = "n";
  static final String COL_REF_INITIAL_POINTER = "p";
  static final String COL_REF_CREATED_AT = "c";
  static final String COL_REF_EXTENDED_INFO = "e";

  static final String COL_VALUE_CONTENT_ID = "i";
  static final String COL_VALUE_PAYLOAD = "p";
  static final String COL_VALUE_DATA = "d";

  static final String COL_SEGMENTS_STRIPES = "s";
  static final String COL_STRIPES_FIRST_KEY = "f";
  static final String COL_STRIPES_LAST_KEY = "l";
  static final String COL_STRIPES_SEGMENT = "s";

  static final String COL_INDEX_INDEX = "i";

  static final String COL_TAG_COMMIT_ID = "i";
  static final String COL_TAG_MESSAGE = "m";
  static final String COL_TAG_HEADERS = "h";
  static final String COL_TAG_SIGNATURE = "s";

  static final String COL_STRING_CONTENT_TYPE = "y";
  static final String COL_STRING_COMPRESSION = "c";
  static final String COL_STRING_FILENAME = "f";
  static final String COL_STRING_PREDECESSORS = "p";
  static final String COL_STRING_TEXT = "t";

  static final String ID_REPO_PATH = ID_PROPERTY_NAME + "." + COL_REPO;
  static final String ID_OBJ_ID_PATH = ID_PROPERTY_NAME + "." + COL_OBJ_ID;
  static final String ID_REFERENCES_NAME_PATH = ID_PROPERTY_NAME + "." + COL_REFERENCES_NAME;

  private MongoDBConstants() {}
}
