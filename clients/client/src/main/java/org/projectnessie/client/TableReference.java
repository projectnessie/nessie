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
package org.projectnessie.client;

import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.projectnessie.model.Namespace;
import org.projectnessie.model.Validation;

public class TableReference {

  static final String ILLEGAL_PATH_MESSAGE =
      "Path must match the syntax 'table-identifier ( '@' reference-name )? ( '#' pointer )?',"
          + " optional enclosed by backticks or single-quotes.";
  static final String ILLEGAL_HASH_MESSAGE =
      "Invalid table name:"
          + " # is only allowed for hashes (reference by timestamp is not supported)";

  private final Namespace namespace;
  private final String name;
  private final String reference;
  private final String hash;
  private final String timestamp;

  private static final Pattern PATH_PATTERN = Pattern.compile("^([^@#]+)(@([^@#]+))?(#([^@#]+))?$");

  @SuppressWarnings({"QsPrivateBeanMembersInspection", "CdiInjectionPointsInspection"})
  private TableReference(
      Namespace namespace, String name, String reference, String hash, String timestamp) {
    this.namespace = namespace;
    this.name = name;
    this.reference = reference;
    this.timestamp = timestamp;
    this.hash = hash;
  }

  public Namespace getNamespace() {
    return namespace;
  }

  public String getName() {
    return name;
  }

  public String getReference() {
    return reference;
  }

  public String getTimestamp() {
    return timestamp;
  }

  public String getHash() {
    return hash;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    if (!namespace.isEmpty()) {
      sb.append(namespace.name()).append('.');
    }
    if (reference != null || timestamp != null || hash != null) {
      sb.append('`').append(name);
      if (reference != null) {
        sb.append('@').append(reference);
      }
      if (hash != null || timestamp != null) {
        sb.append('#').append(hash != null ? hash : timestamp);
      }
      sb.append('`');
    } else {
      sb.append(name);
    }
    return sb.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TableReference that = (TableReference) o;
    return Objects.equals(namespace, that.namespace)
        && Objects.equals(name, that.name)
        && Objects.equals(timestamp, that.timestamp)
        && Objects.equals(reference, that.reference)
        && Objects.equals(hash, that.hash);
  }

  @Override
  public int hashCode() {
    return Objects.hash(namespace, name, timestamp, reference, hash);
  }

  /**
   * Convert a namespace and the string representation of a table-reference into a {@link
   * TableReference}.
   *
   * @param namespace namespace to return in the {@link TableReference} as is
   * @param name name being parsed via {@link #parseEmptyNamespace(String)}
   */
  public static TableReference parse(String[] namespace, String name) {
    return parse(Namespace.of(namespace), name);
  }

  /**
   * Convert a namespace and the string representation of a table-reference into a {@link
   * TableReference}.
   *
   * @param namespace namespace to return in the {@link TableReference} as is
   * @param name name being parsed via {@link #parseEmptyNamespace(String)}
   */
  public static TableReference parse(List<String> namespace, String name) {
    return parse(Namespace.of(namespace), name);
  }

  /**
   * Convert a namespace and the string representation of a table-reference into a {@link
   * TableReference}.
   *
   * @param namespace namespace to return in the {@link TableReference} as is
   * @param name name being parsed via {@link #parseEmptyNamespace(String)}
   */
  public static TableReference parse(Namespace namespace, String name) {
    TableReference noNamespace = parseEmptyNamespace(name);
    return new TableReference(
        namespace,
        noNamespace.getName(),
        noNamespace.getReference(),
        noNamespace.getHash(),
        noNamespace.getTimestamp());
  }

  /**
   * Parses a "path" to a {@link TableReference} without a namespace.
   *
   * <p>The syntax is <br>
   * {@code table-identifier ( '@' reference-name )? ( '#' pointer )?}
   *
   * <p>{@code table-identifier} is the table-identifier.
   *
   * <p>{@code reference-name} is the optional name of a branch or tag in Nessie.
   *
   * <p>{@code pointer} is the optional Nessie commit-hash within the named reference. Defaults to
   * the "HEAD" of the named reference.
   */
  public static TableReference parseEmptyNamespace(String path) {
    String unquoted = unquote(path);
    Matcher matcher = PATH_PATTERN.matcher(unquoted);
    if (!matcher.matches()) {
      throw new IllegalArgumentException(ILLEGAL_PATH_MESSAGE);
    }

    String table = matcher.group(1);
    String refName = matcher.group(3);
    String hash = matcher.group(5);

    if (refName != null) {
      Validation.validateReferenceName(refName);
    }
    if (hash != null && !Validation.isValidHash(hash)) {
      throw new IllegalArgumentException(ILLEGAL_HASH_MESSAGE);
    }

    return new TableReference(Namespace.EMPTY, table, refName, hash, null);
  }

  /**
   * When passing the string representation of a table-identifier like {@code
   * foo.'table_name@my_branch'} into e.g. {@code
   * SparkSession.createDataFrame().writeTo("foo.'table_name@my_branch'")}, strip the surrounding
   * quotes.
   */
  private static String unquote(String path) {
    if (path.startsWith("`") && path.endsWith("`")) {
      return path.substring(1, path.length() - 1);
    } else if (path.startsWith("'") && path.endsWith("'")) {
      return path.substring(1, path.length() - 1);
    }
    return path;
  }
}
