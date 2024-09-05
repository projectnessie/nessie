/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.versioned.storage.mongodb2;

final class MongoDB2Constants {

  static final String TABLE_REFS = "refs2";
  static final String TABLE_OBJS = "objs2";

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
  static final String COL_OBJ_VERS = "V";
  static final String COL_OBJ_VALUE = "v";
  static final String COL_OBJ_REFERENCED = "z";

  static final String ID_REPO_PATH = ID_PROPERTY_NAME + "." + COL_REPO;

  private MongoDB2Constants() {}
}
