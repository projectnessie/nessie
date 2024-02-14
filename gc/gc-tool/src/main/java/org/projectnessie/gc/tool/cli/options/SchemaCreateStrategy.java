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
package org.projectnessie.gc.tool.cli.options;

import java.sql.Connection;
import java.sql.SQLException;
import org.projectnessie.gc.contents.jdbc.JdbcHelper;

public enum SchemaCreateStrategy {
  CREATE {
    @Override
    public void apply(Connection conn) throws SQLException {
      JdbcHelper.createTables(conn, false);
    }
  },
  DROP_AND_CREATE {
    @Override
    public void apply(Connection conn) throws SQLException {
      JdbcHelper.dropTables(conn);
      JdbcHelper.createTables(conn, false);
    }
  },
  CREATE_IF_NOT_EXISTS {
    @Override
    public void apply(Connection conn) throws SQLException {
      JdbcHelper.createTables(conn, true);
    }
  },
  ;

  public abstract void apply(Connection conn) throws SQLException;
}
