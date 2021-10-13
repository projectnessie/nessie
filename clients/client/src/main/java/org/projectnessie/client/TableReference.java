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

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.projectnessie.model.ContentsKey;
import org.projectnessie.model.Namespace;
import org.projectnessie.model.Validation;

public class TableReference {

  static final String ILLEGAL_PATH_MESSAGE =
      "Path must match the syntax 'table-identifier ( '@' reference-name )? ( '#' pointer )?',"
          + " optional enclosed by backticks or single-quotes.";
  static final String ILLEGAL_HASH_MESSAGE =
      "Invalid table name:"
          + " # is only allowed for hashes (reference by timestamp is not supported)";

  private final ContentsKey contentsKey;
  private final String timestamp;
  private final String reference;
  private final String hash;

  private static final Pattern PATH_PATTERN = Pattern.compile("^([^@#]+)(@([^@#]+))?(#([^@#]+))?$");

  /**
   * Container class to specify a TableIdentifier on a specific Reference or at an Instant in time.
   */
  @SuppressWarnings({"QsPrivateBeanMembersInspection", "CdiInjectionPointsInspection"})
  private TableReference(ContentsKey contentsKey, String timestamp, String reference, String hash) {
    this.contentsKey = contentsKey;
    this.timestamp = timestamp;
    this.reference = reference;
    this.hash = hash;
  }

  public ContentsKey contentsKey() {
    return contentsKey;
  }

  public String reference() {
    return reference;
  }

  public String timestamp() {
    return timestamp;
  }

  public String hash() {
    return hash;
  }

  /** Convert dataset read/write options to a table and ref/hash. */
  public static TableReference parse(Namespace namespace, String name) {
    TableReference noNamespace = parse(name);
    return new TableReference(
        ContentsKey.of(namespace, noNamespace.contentsKey().getName()),
        noNamespace.timestamp(),
        noNamespace.reference(),
        noNamespace.hash());
  }

  /**
   * Parses a "path" to a {@link TableReference}.
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
  public static TableReference parse(String path) {
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

    ContentsKey contentsKey = ContentsKey.fromPathString(table);
    return new TableReference(contentsKey, null, refName, hash);
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
