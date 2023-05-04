/*
 * Copyright (C) 2023 Dremio
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

import static com.google.common.base.Strings.repeat;
import static org.assertj.core.api.Assertions.assertThat;
import static org.projectnessie.gc.contents.jdbc.JdbcPersistenceSpi.trimError;
import static org.projectnessie.gc.contents.jdbc.SqlDmlDdl.ERROR_LENGTH;

import org.junit.jupiter.api.Test;

public class TestJdbcPersistenceUnit {
  @Test
  void failureTextTrim() {
    assertThat(trimError("foo")).isEqualTo("foo");
    String s2000 = repeat("x", ERROR_LENGTH);
    assertThat(trimError(s2000)).isSameAs(s2000);
    assertThat(trimError(repeat("x", ERROR_LENGTH + 1)))
        .startsWith("xxxxxxx")
        .endsWith(" ... (truncated)")
        .hasSize(ERROR_LENGTH);
  }
}
