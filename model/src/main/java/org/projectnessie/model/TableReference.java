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
package org.projectnessie.model;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import org.immutables.value.Value;

/**
 * Functionality to parse a table-reference string representation containing the table name plus
 * optional reference name, commit hash or timestamp.
 *
 * <p>The syntax is <br>
 * {@code table-identifier ( '@' reference-name )? ( '#' hashOrTimestamp )?}
 *
 * <p>{@code table-identifier} is the table-identifier.
 *
 * <p>{@code reference-name} is the optional name of a branch or tag in Nessie.
 *
 * <p>{@code hashOrTimestamp} is the optional Nessie commit-hash or a timestamp within the named
 * reference. If {@code hashOrTimestamp} represents a valid commit hash, it is interpreted as one,
 * otherwise it represents a timestamp.
 */
@Value.Immutable
public abstract class TableReference {

  static final String ILLEGAL_TABLE_REFERENCE_MESSAGE =
      "Illegal table reference syntax, '%s' must match the syntax "
          + "'table-identifier ( '@' reference-name )? ( '#' hashOrTimestamp )?',"
          + " optionally enclosed by backticks or single-quotes.";

  private static final Pattern TABLE_REFERENCE_PATTERN =
      Pattern.compile("^([^@#]+)(@([^@#]+))?(#([^@#]+))?$");

  @NotNull
  @NotEmpty
  public abstract String getName();

  @Nullable
  @NotEmpty
  public abstract String getReference();

  @Value.Redacted
  public boolean hasReference() {
    return getReference() != null;
  }

  @Nullable
  @NotEmpty
  public abstract String getTimestamp();

  @Value.Redacted
  public boolean hasTimestamp() {
    return getTimestamp() != null;
  }

  @Nullable
  @NotEmpty
  public abstract String getHash();

  @Value.Redacted
  public boolean hasHash() {
    return getHash() != null;
  }

  @Override
  public String toString() {
    if (hasReference() || hasTimestamp() || hasHash()) {
      StringBuilder sb = new StringBuilder();
      sb.append('`').append(getName());
      if (hasReference()) {
        sb.append('@').append(getReference());
      }
      if (hasHash()) {
        sb.append('#').append(getHash());
      } else if (hasTimestamp()) {
        sb.append('#').append(getTimestamp());
      }
      sb.append('`');
      return sb.toString();
    }
    return getName();
  }

  /** Parses the string representation of a table-reference to a {@link TableReference} object. */
  public static TableReference parse(String tableReference) {
    String unquoted = unquote(tableReference);
    Matcher matcher = TABLE_REFERENCE_PATTERN.matcher(unquoted);
    if (!matcher.matches()) {
      throw new IllegalArgumentException(
          String.format(ILLEGAL_TABLE_REFERENCE_MESSAGE, tableReference));
    }

    String table = matcher.group(1);
    String refName = matcher.group(3);
    String hashOrTimestamp = matcher.group(5);

    if (refName != null) {
      Validation.validateReferenceName(refName);
    }
    ImmutableTableReference.Builder b = ImmutableTableReference.builder().name(table);
    if (refName != null) {
      b.reference(refName);
    }
    if (hashOrTimestamp != null) {
      if (Validation.isValidHash(hashOrTimestamp)) {
        b.hash(hashOrTimestamp);
      } else {
        b.timestamp(hashOrTimestamp);
      }
    }
    return b.build();
  }

  /**
   * When passing the string representation of a table-identifier like {@code
   * foo.'table_name@my_branch'} into e.g. {@code
   * SparkSession.createDataFrame().writeTo("foo.'table_name@my_branch'")}, strip the surrounding
   * quotes.
   */
  private static String unquote(String maybeQuoted) {
    if ((maybeQuoted.startsWith("`") && maybeQuoted.endsWith("`"))
        || (maybeQuoted.startsWith("'") && maybeQuoted.endsWith("'"))) {
      return maybeQuoted.substring(1, maybeQuoted.length() - 1);
    }
    return maybeQuoted;
  }
}
