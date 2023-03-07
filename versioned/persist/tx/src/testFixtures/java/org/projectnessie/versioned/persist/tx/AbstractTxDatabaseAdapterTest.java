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
package org.projectnessie.versioned.persist.tx;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.tuple;

import java.sql.SQLException;
import java.sql.Statement;
import org.junit.jupiter.api.Test;
import org.projectnessie.versioned.persist.tests.AbstractDatabaseAdapterTest;
import org.projectnessie.versioned.persist.tx.TxDatabaseAdapter.NessieSqlDataType;

public abstract class AbstractTxDatabaseAdapterTest extends AbstractDatabaseAdapterTest {

  @Override
  protected boolean commitWritesValidated() {
    return true;
  }

  @Test
  void insertOnConflict() throws Exception {
    TxDatabaseAdapter txDatabaseAdapter = (TxDatabaseAdapter) databaseAdapter;
    String stringType = txDatabaseAdapter.databaseSqlFormatParameters().get(NessieSqlDataType.HASH);
    try (ConnectionWrapper conn = txDatabaseAdapter.borrowConnection()) {
      try (Statement st = conn.conn().createStatement()) {
        st.execute(
            String.format(
                "CREATE TABLE is_integrity_constraint_violation (id %s PRIMARY KEY, val %s)",
                stringType, stringType));

        String insertConflictRaw =
            "INSERT INTO is_integrity_constraint_violation (id, val) VALUES ('123', '456')";
        String insertConflict = txDatabaseAdapter.insertOnConflictDoNothing(insertConflictRaw);

        st.execute(insertConflict);
        if (insertConflict.equals(insertConflictRaw)) {
          assertThatThrownBy(() -> st.execute(insertConflict))
              .isInstanceOf(SQLException.class)
              .extracting(SQLException.class::cast)
              .extracting(e -> tuple(e, e.getErrorCode(), e.getSQLState(), e.getMessage()))
              .matches(
                  t ->
                      ((TxDatabaseAdapter) databaseAdapter)
                          .isIntegrityConstraintViolation((Throwable) t.toList().get(0)));
        } else {
          st.execute(insertConflict);
        }

        st.execute("INSERT INTO is_integrity_constraint_violation (id, val) VALUES ('234', '567')");
      }
    }
  }
}
