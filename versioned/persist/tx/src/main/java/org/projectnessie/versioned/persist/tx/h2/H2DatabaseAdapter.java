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
package org.projectnessie.versioned.persist.tx.h2;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.projectnessie.versioned.persist.tx.TxDatabaseAdapter;
import org.projectnessie.versioned.persist.tx.TxDatabaseAdapterConfig;

public class H2DatabaseAdapter extends TxDatabaseAdapter {

  public H2DatabaseAdapter(TxDatabaseAdapterConfig config) {
    super(config);
  }

  @Override
  protected Map<SqlDataType, String> databaseSqlFormatParameters() {
    return ImmutableMap.<SqlDataType, String>builder()
        .put(SqlDataType.BLOB, "VARBINARY(390000)")
        .put(SqlDataType.HASH, "VARCHAR")
        .put(SqlDataType.KEY_PREFIX, "VARCHAR")
        .put(SqlDataType.KEY, "VARCHAR")
        .put(SqlDataType.NAMED_REF, "VARCHAR")
        .put(SqlDataType.NAMED_REF_TYPE, "VARCHAR")
        .put(SqlDataType.CONTENTS_ID, "VARCHAR")
        .put(SqlDataType.INTEGER, "BIGINT")
        .build();
  }
}
