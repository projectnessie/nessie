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
package org.projectnessie.hms.apis;

import java.util.List;

import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.projectnessie.hms.annotation.NoopThrow;
import org.projectnessie.hms.annotation.Route;

public interface AnnotatedHive2RawStore extends BaseRawStoreUnion {

  @NoopThrow
  void addForeignKeys(@Route List<SQLForeignKey> fks) throws InvalidObjectException, MetaException;

  @NoopThrow
  void addPrimaryKeys(@Route List<SQLPrimaryKey> pks) throws InvalidObjectException, MetaException;

}
