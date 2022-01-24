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
package org.apache.iceberg.view;

import java.util.List;
import java.util.Objects;
import org.apache.iceberg.Schema;

/** SQL metadata for a view. */
class BaseViewDefinition implements ViewDefinition {
  private final String sql;
  private final Schema schema;
  private final String sessionCatalog;
  private final List<String> sessionNamespace;

  public BaseViewDefinition(
      String sql, Schema schema, String sessionCatalog, List<String> sessionNamespace) {
    this.sql = sql;
    this.schema = schema;
    this.sessionCatalog = sessionCatalog;
    this.sessionNamespace = sessionNamespace;
  }

  @Override
  public String sql() {
    return sql;
  }

  @Override
  public Schema schema() {
    return schema;
  }

  @Override
  public String sessionCatalog() {
    return sessionCatalog;
  }

  @Override
  public List<String> sessionNamespace() {
    return sessionNamespace;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    BaseViewDefinition that = (BaseViewDefinition) o;
    return Objects.equals(sql, that.sql)
        // FIXME: include schema in hashCode+equals?
        // && Objects.equals(schema, that.schema)
        && Objects.equals(sessionCatalog, that.sessionCatalog)
        && Objects.equals(sessionNamespace, that.sessionNamespace);
  }

  @Override
  public int hashCode() {
    return Objects.hash(sql, sessionCatalog, sessionNamespace);
  }

  @Override
  public String toString() {
    return "BaseViewDefinition{"
        + "sql='"
        + sql
        + '\''
        + ", schema="
        + schema
        + ", sessionCatalog='"
        + sessionCatalog
        + '\''
        + ", sessionNamespace="
        + sessionNamespace
        + '}';
  }
}
