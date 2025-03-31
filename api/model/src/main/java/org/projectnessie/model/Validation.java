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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Collection of validation rules. */
public final class Validation {

  // Note: when changing a regex here, also update it in python/
  public static final String HASH_RAW_REGEX = "[0-9a-fA-F]{8,64}";
  public static final String HASH_REGEX = "^" + HASH_RAW_REGEX + "$";

  public static final String RELATIVE_COMMIT_SPEC_RAW_REGEX =
      "([~*^])([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}([.][0-9]{1,9})?Z|([0-9]+))";

  /**
   * Regex with an optional hash and a sequence of relative lookups, which can be by-timestamp,
   * by-n-th-predecessor or by-n-th-parent.
   *
   * <ul>
   *   <li>Lookup by timestamp starts with {@code *} followed by the numeric value of the timestamp
   *       in milliseconds since epoch.
   *   <li>Lookup by n-th predecessor starts with {@code ~} followed by the value for the n-th
   *       commit in the commit log.
   *   <li>Lookup by n-th parent starts with {@code ^} followed by either 1, referencing the direct
   *       parent, or 2, referencing the merge parent.
   * </ul>
   *
   * <p>Valid values are:
   *
   * <ul>
   *   <li>{@code 11223344~10} -> the 10th parent of the commit {@code 11223344}
   *   <li>{@code 11223344^2} -> the merge parent of the commit {@code 11223344}
   *   <li>{@code 11223344~10^2} -> the merge parent of the 10th parent of the commit {@code
   *       11223344}
   *   <li>{@code 11223344~10^1} -> the direct parent of the 10th parent of the commit {@code
   *       11223344} - functionally equal to {@code 11223344~11}
   *   <li>{@code 11223344*10000000000} -> the commit in the commit log starting at {@code 11223344}
   *       with a commit-created timestamp of {@code 10000000000} or less.
   *   <li>{@code 11223344*2021-04-07T14:42:25.534748Z} -> the commit in the commit log starting at
   *       {@code 11223344} with a commit-created timestamp of {@code 2021-04-07T14:42:25.534748Z}
   *       or less.
   * </ul>
   */
  public static final String HASH_OR_RELATIVE_COMMIT_SPEC_RAW_REGEX =
      "(" + HASH_RAW_REGEX + ")?((?:" + RELATIVE_COMMIT_SPEC_RAW_REGEX + ")*)";

  public static final String HASH_OR_RELATIVE_COMMIT_SPEC_REGEX =
      "^" + HASH_OR_RELATIVE_COMMIT_SPEC_RAW_REGEX + "$";

  public static final String REF_NAME_RAW_REGEX =
      "(?:[A-Za-z](?:(?:(?![.][.])[A-Za-z0-9./_-])*[A-Za-z0-9_-])?)|-";
  public static final String REF_NAME_REGEX = "^" + REF_NAME_RAW_REGEX + "$";

  public static final String REF_TYPE_RAW_REGEX = "BRANCH|branch|TAG|tag";
  public static final String REF_TYPE_REGEX = "^(" + REF_TYPE_RAW_REGEX + ")$";
  public static final String REF_NAME_OR_HASH_REGEX =
      "^(?:(" + HASH_RAW_REGEX + ")|(" + REF_NAME_RAW_REGEX + "))$";
  public static final String REF_NAME_PATH_REGEX =
      "^("
          + REF_NAME_RAW_REGEX
          + ")?(?:@("
          + HASH_RAW_REGEX
          + ")?)?("
          + RELATIVE_COMMIT_SPEC_RAW_REGEX
          + ")*$";
  public static final String REF_NAME_PATH_ELEMENT_REGEX = "([^/]+|[^@]+(@|%40)[^@/]*)";

  public static final Pattern HASH_PATTERN = Pattern.compile(HASH_REGEX);
  public static final Pattern REF_NAME_PATTERN = Pattern.compile(REF_NAME_REGEX);
  public static final Pattern RELATIVE_COMMIT_SPEC_PART_PATTERN =
      Pattern.compile(RELATIVE_COMMIT_SPEC_RAW_REGEX);
  public static final Pattern HASH_OR_RELATIVE_COMMIT_SPEC_PATTERN =
      Pattern.compile(HASH_OR_RELATIVE_COMMIT_SPEC_REGEX);
  public static final Pattern REF_NAME_OR_HASH_PATTERN = Pattern.compile(REF_NAME_OR_HASH_REGEX);
  public static final Pattern REF_NAME_PATH_PATTERN = Pattern.compile(REF_NAME_PATH_REGEX);
  public static final Pattern REF_NAME_PATH_ELEMENT_PATTERN =
      Pattern.compile(REF_NAME_PATH_ELEMENT_REGEX);

  /*
   * Default Cut-Off Policy can be a number or Duration or ISO instant.
   * Following rules are to validate the same.
   * */
  private static final String DURATION_REGEX =
      "([-+]?)P(?:([-+]?[0-9]+)D)?(T(?:([-+]?[0-9]+)H)?(?:([-+]?[0-9]+)M)?(?:([-+]?[0-9]+)(?:[.,]([0-9]{0,9}))?S)?)?";

  private static final String ISO_TIME_REGEX =
      "^(?:[1-9]\\d{3}-(?:(?:0[1-9]|1[0-2])-(?:0[1-9]|1\\d|2[0-8])|(?:0[13-9]|1[0-2])-(?:29|30)|(?:0[13578]|1[02])-31)|(?:[1-9]\\d(?:0[48]|[2468][048]|[13579][26])|(?:[2468][048]|[13579][26])00)-02-29)T(?:[01]\\d|2[0-3]):[0-5]\\d:[0-5]\\d(?:\\.\\d{1,9})?(?:Z|[+-][01]\\d:[0-5]\\d)$";

  private static final String POSITIVE_INTEGER_REGEX = "^[1-9]\\d{0,10}";
  public static final String DEFAULT_CUT_OFF_POLICY_REGEX =
      "NONE" + "|" + POSITIVE_INTEGER_REGEX + "|" + DURATION_REGEX + "|" + ISO_TIME_REGEX;
  public static final String DEFAULT_CUT_OFF_POLICY_MESSAGE =
      "Default cut-off-policy must be either the number of commits, a duration (as per java.time.Duration) or an ISO instant (like 2011-12-03T10:15:30Z) ";
  public static final Pattern DEFAULT_CUT_OFF_POLICY_PATTERN =
      Pattern.compile(DEFAULT_CUT_OFF_POLICY_REGEX, Pattern.CASE_INSENSITIVE);

  public static final String HASH_RULE = "consist of the hex representation of 4-32 bytes";
  private static final String REF_RULE =
      "start with a letter, followed by letters, digits, one of the ./_- characters, "
          + "not end with a slash or dot, not contain '..'";

  public static final String HASH_MESSAGE = "Hash must " + HASH_RULE;

  public static final String RELATIVE_COMMIT_SPEC_RULE =
      "be either "
          + "'~' + a number representing the n-th predecessor of a commit, "
          + "'^' + a number representing the n-th parent within a commit, or "
          + "'*' + a number representing the created timestamp of a commit, in milliseconds since epoch or in ISO-8601 format";
  public static final String HASH_OR_RELATIVE_COMMIT_SPEC_RULE =
      "consist of either a valid commit hash (which in turn must "
          + HASH_RULE
          + "), or a valid relative part (which must "
          + RELATIVE_COMMIT_SPEC_RULE
          + "), or both.";
  public static final String HASH_OR_RELATIVE_COMMIT_SPEC_MESSAGE =
      "Hash with optional relative part must " + HASH_OR_RELATIVE_COMMIT_SPEC_RULE;

  public static final String REF_NAME_PATH_MESSAGE =
      "Reference name must "
          + REF_RULE
          + ", optionally followed "
          + "by @ and a hash with optional relative part. "
          + HASH_OR_RELATIVE_COMMIT_SPEC_MESSAGE;
  public static final String REF_NAME_MESSAGE = "Reference name must " + REF_RULE;

  public static final String REF_TYPE_RULE = "be either 'branch' or 'tag'";
  public static final String REF_TYPE_MESSAGE = "Reference type name must " + REF_TYPE_RULE;

  public static final String REF_NAME_OR_HASH_MESSAGE =
      "Reference must be either a reference name or hash, " + REF_RULE + " or " + HASH_RULE;
  public static final String FORBIDDEN_REF_NAME_MESSAGE =
      "Reference name mut not be HEAD, DETACHED or a potential commit ID representation.";
  public static final Set<String> FORBIDDEN_REF_NAMES =
      Collections.unmodifiableSet(new HashSet<>(Arrays.asList("HEAD", "DETACHED")));

  private Validation() {
    // empty
  }

  /**
   * Just checks whether a string is a valid reference-name, but doesn't throw an exception.
   *
   * @see #validateReferenceName(String)
   */
  public static boolean isValidReferenceName(String referenceName) {
    Objects.requireNonNull(referenceName, "referenceName must not be null");
    Matcher matcher = REF_NAME_PATTERN.matcher(referenceName);
    return matcher.matches();
  }

  /**
   * Just checks whether a string is a valid hash, but doesn't throw an exception.
   *
   * @see #validateHash(String)
   */
  public static boolean isValidHash(String hash) {
    Objects.requireNonNull(hash, "hash must not be null");
    Matcher matcher = HASH_PATTERN.matcher(hash);
    return matcher.matches();
  }

  /**
   * Just checks whether a string is a valid hash and/or a relative spec, but doesn't throw an
   * exception.
   *
   * @see #validateHashOrRelativeSpec(String)
   */
  public static boolean isValidHashOrRelativeSpec(String hash) {
    Objects.requireNonNull(hash, "hash must not be null");
    if (hash.isEmpty()) {
      return false;
    }
    Matcher matcher = HASH_OR_RELATIVE_COMMIT_SPEC_PATTERN.matcher(hash);
    return matcher.matches();
  }

  /**
   * Just checks whether a string is a valid reference-name (as per {@link
   * #isValidReferenceName(String)}) or a valid hash (as per {@link #isValidHash(String)}).
   */
  public static boolean isValidReferenceNameOrHash(String ref) {
    Objects.requireNonNull(ref, "reference (name or hash) must not be null");
    Matcher matcher = REF_NAME_OR_HASH_PATTERN.matcher(ref);
    return matcher.matches();
  }

  /**
   * Validates whether a string is a valid reference-name.
   *
   * <p>The rules are: <em>{@value #REF_RULE}</em>
   *
   * @param referenceName the reference name string to test.
   */
  public static String validateReferenceName(String referenceName) throws IllegalArgumentException {
    if (isValidReferenceName(referenceName)) {
      return referenceName;
    }
    throw new IllegalArgumentException(REF_NAME_MESSAGE + " - but was: " + referenceName);
  }

  /**
   * Validates whether a string is a valid hash.
   *
   * <p>The rules are: <em>{@value #HASH_RULE}</em>
   *
   * @param referenceName the reference name string to test.
   */
  public static String validateHash(String referenceName) throws IllegalArgumentException {
    if (isValidHash(referenceName)) {
      return referenceName;
    }
    throw new IllegalArgumentException(HASH_MESSAGE + " - but was: " + referenceName);
  }

  /**
   * Validates whether a string is a valid hash with optional relative specs.
   *
   * <p>The rules are: <em>{@value #HASH_OR_RELATIVE_COMMIT_SPEC_RULE}</em>
   *
   * @param referenceName the reference name string to test.
   */
  public static String validateHashOrRelativeSpec(String referenceName)
      throws IllegalArgumentException {
    if (isValidHashOrRelativeSpec(referenceName)) {
      return referenceName;
    }
    throw new IllegalArgumentException(
        HASH_OR_RELATIVE_COMMIT_SPEC_MESSAGE + " - but was: " + referenceName);
  }

  /** Just checks whether a string is a valid cut-off policy or not. */
  private static boolean isValidDefaultCutoffPolicy(String policy) {
    Objects.requireNonNull(policy, "policy must not be null");
    Matcher matcher = DEFAULT_CUT_OFF_POLICY_PATTERN.matcher(policy);
    return matcher.matches();
  }

  /**
   * Validate default cutoff policy. Policies can be one of: - number of commits as an integer
   * value, - a duration (see java.time.Duration), - an ISO instant, - 'NONE,' means everything's
   * considered as live
   */
  public static void validateDefaultCutOffPolicy(String value) {
    if (isValidDefaultCutoffPolicy(value)) {
      return;
    }
    throw new IllegalArgumentException(
        "Failed to parse default-cutoff-value '" + value + "'," + DEFAULT_CUT_OFF_POLICY_MESSAGE);
  }

  /**
   * Validates whether a string is a valid reference name or hash.
   *
   * <p>See {@link #validateReferenceName(String)} and {@link #validateHash(String)} for the rules.
   *
   * @param ref the reference name string to test.
   */
  public static String validateReferenceNameOrHash(String ref) throws IllegalArgumentException {
    if (isValidReferenceNameOrHash(ref)) {
      return ref;
    }
    throw new IllegalArgumentException(REF_NAME_OR_HASH_MESSAGE + " - but was: " + ref);
  }

  /**
   * Checks whether {@code ref} represents a forbidden reference name ({@code HEAD} or {@code
   * DETACHED}) or could represent a commit-ID.
   *
   * @param ref reference name to check
   * @return {@code true}, if forbidden
   */
  public static boolean isForbiddenReferenceName(String ref) {
    return FORBIDDEN_REF_NAMES.contains(ref.toUpperCase(Locale.ROOT))
        || HASH_PATTERN.matcher(ref).matches();
  }

  /**
   * Throws an {@link IllegalArgumentException} if {@code ref} represents a forbidden reference
   * name, see {@link #isForbiddenReferenceName(String)}.
   *
   * @param ref reference name to check
   * @return {@code ref}
   */
  public static String validateForbiddenReferenceName(String ref) throws IllegalArgumentException {
    if (isForbiddenReferenceName(ref)) {
      throw new IllegalArgumentException(FORBIDDEN_REF_NAME_MESSAGE + " - but was " + ref);
    }
    return ref;
  }
}
