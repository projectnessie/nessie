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
package org.projectnessie.versioned.tiered.hsql;

import java.util.Arrays;
import java.util.List;
import org.projectnessie.versioned.tiered.tx.TxDatabaseAdapter;
import org.projectnessie.versioned.tiered.tx.TxDatabaseAdapterConfig;

public class HSqlDatabaseAdapter extends TxDatabaseAdapter {

  public HSqlDatabaseAdapter(TxDatabaseAdapterConfig config) {
    super(config);
  }

  @Override
  protected List<String> databaseSqlFormatParameters() {
    return Arrays.asList(
        // BLOB column
        "VARBINARY(390000)",
        // Hash
        "VARCHAR(80)",
        // key-prefix
        "VARCHAR(120)",
        // Key
        "VARCHAR(200)",
        // NamedRef,
        "VARCHAR(200)",
        // named-reference type
        "VARCHAR(1)");
  }
}
