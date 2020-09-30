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
package com.dremio.nessie.iceberg;

import static com.dremio.nessie.iceberg.NessieCatalog.CONF_NESSIE_HASH;
import static com.dremio.nessie.iceberg.NessieCatalog.CONF_NESSIE_REF;

import java.util.Map;

import org.apache.iceberg.catalog.TableIdentifier;

public class ParsedTableIdentifier {

  private final TableIdentifier tableIdentifier;
  private final String hash;
  private final String reference;

  /**
   * container class to hold all options in a Nessie table name.
   */
  public ParsedTableIdentifier(TableIdentifier tableIdentifier, String hash, String reference) {
    this.tableIdentifier = tableIdentifier;
    this.hash = hash;
    this.reference = reference;
  }

  public TableIdentifier getTableIdentifier() {
    return tableIdentifier;
  }

  public String getHash() {
    return hash;
  }

  public String getReference() {
    return reference;
  }


  /**
   * Convert dataset read/write options to a table and ref/hash.
   */
  public static ParsedTableIdentifier getParsedTableIdentifier(String path, Map<String, String> properties) {
    //I am assuming tables can't have @ or # symbols
    if (path.split("@").length > 2) {
      throw new IllegalArgumentException(String.format("Can only reference one branch in %s", path));
    }
    if (path.split("#").length > 2) {
      throw new IllegalArgumentException(String.format("Can only reference one hash in %s", path));
    }

    if (path.contains("@") && path.contains("#")) {
      String[] tableRef = path.split("@");
      TableIdentifier identifier = TableIdentifier.parse(tableRef[0]);
      String[] refHash = tableRef[1].split("#");
      return new ParsedTableIdentifier(identifier, refHash[1], refHash[0]);
    }

    if (path.contains("@")) {
      String[] tableRef = path.split("@");
      TableIdentifier identifier = TableIdentifier.parse(tableRef[0]);
      return new ParsedTableIdentifier(identifier, null, tableRef[1]);
    }

    if (path.contains("#")) {
      String[] tableRef = path.split("#");
      TableIdentifier identifier = TableIdentifier.parse(tableRef[0]);
      return new ParsedTableIdentifier(identifier, tableRef[1], null);
    }

    TableIdentifier identifier = TableIdentifier.parse(path);
    String reference = properties.get(CONF_NESSIE_REF);
    String hash = properties.get(CONF_NESSIE_HASH);
    return new ParsedTableIdentifier(identifier, hash, reference);
  }

  /**
   * Convert dataset read/write options to a table and ref/hash.
   */
  public static ParsedTableIdentifier getParsedTableIdentifier(TableIdentifier path, Map<String, String> properties) {
    ParsedTableIdentifier pti = getParsedTableIdentifier(path.name(), properties);
    return new ParsedTableIdentifier(TableIdentifier.of(path.namespace(), pti.getTableIdentifier().name()), pti.getHash(),
                                     pti.getReference());
  }
}
