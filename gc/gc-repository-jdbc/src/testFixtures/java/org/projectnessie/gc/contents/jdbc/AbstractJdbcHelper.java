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
package org.projectnessie.gc.contents.jdbc;

import javax.sql.DataSource;
import org.junit.jupiter.api.AfterAll;

public class AbstractJdbcHelper {

  protected static DataSource dataSource;

  static void initDataSource(String jdbcUrl) throws Exception {
    AgroalJdbcDataSourceProvider dsProvider =
        AgroalJdbcDataSourceProvider.builder()
            .jdbcUrl(jdbcUrl)
            .usernamePasswordCredentials("test", "test")
            .poolMinSize(1)
            .poolMaxSize(1)
            .poolInitialSize(1)
            .build();
    dataSource = dsProvider.dataSource();
  }

  @AfterAll
  static void closeDataSource() throws Exception {
    if (dataSource instanceof AutoCloseable) {
      ((AutoCloseable) dataSource).close();
    }
  }
}
