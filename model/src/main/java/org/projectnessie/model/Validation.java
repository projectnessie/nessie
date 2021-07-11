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

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;

import java.util.Collection;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Collection of validation rules. */
public final class Validation {
  public static final String OWNER_REGEX = "^[A-Za-z](((?![.][.])[A-Za-z0-9._-])*[A-Za-z0-9._-])?$";
  public static final String REPO_REGEX = "^[A-Za-z](((?![.][.])[A-Za-z0-9._-])*[A-Za-z0-9._-])?$";
  public static final String HASH_REGEX = "^[0-9a-fA-F]{16,64}$";
  public static final String REF_NAME_REGEX =
      "^[A-Za-z](((?![.][.])[A-Za-z0-9./_-])*[A-Za-z0-9._-])?$";
  public static final String REF_NAME_OR_HASH_REGEX =
      "^(([0-9a-fA-F]{16,64})|([A-Za-z](((?![.][.])[A-Za-z0-9./_-])*[A-Za-z0-9._-])?))$";

  public static final Pattern REPO_PATTERN = Pattern.compile(REPO_REGEX);
  public static final Pattern OWNER_PATTERN = Pattern.compile(OWNER_REGEX);
  public static final Pattern HASH_PATTERN = Pattern.compile(HASH_REGEX);
  public static final Pattern REF_NAME_PATTERN = Pattern.compile(REF_NAME_REGEX);
  public static final Pattern REF_NAME_OR_HASH_PATTERN = Pattern.compile(REF_NAME_OR_HASH_REGEX);

  /** Invalid owner/repo names that represent current and reserved URI path elements. */
  public static final Collection<String> INVALID_REPO_IDS =
      unmodifiableList(
          asList("config", "contents", "trees")); // just 2 elements, so a list is not worse

  private static final String REPO_RULE =
      "start with a letter, followed by letters, digits, ._- "
          + "characters and not be equal to 'config', 'contents', 'trees'";
  private static final String OWNER_RULE =
      "start with a letter, followed by letters, digits, ._- "
          + "characters and not be equal to 'config', 'contents', 'trees'";
  private static final String HASH_RULE = "consist of the hex representation of 8-32 bytes";
  private static final String REF_RULE =
      "start with a letter, followed by letters, digits, a ./_- character, "
          + "not end with a slash, not contain ..";

  public static final String REPO_MESSAGE = "Repository name must " + REPO_RULE;
  public static final String OWNER_MESSAGE = "Owner name must " + OWNER_RULE;
  public static final String HASH_MESSAGE = "Hash must " + HASH_RULE;
  public static final String REF_NAME_MESSAGE = "Reference name must " + REF_RULE;
  public static final String REF_NAME_OR_HASH_MESSAGE =
      "Reference must be either a reference name or hash, " + REF_RULE + " or " + HASH_RULE;

  private Validation() {
    // empty
  }

  /**
   * Just checks whether a string is a valid repository owner, but doesn't throw an exception.
   *
   * @see #validateOwner(String)
   */
  public static boolean isValidOwner(String repoOwner) {
    Objects.requireNonNull(repoOwner, "repo must not be null");
    Matcher matcher = Validation.OWNER_PATTERN.matcher(repoOwner);
    return matcher.matches() && !INVALID_REPO_IDS.contains(repoOwner);
  }

  /**
   * Validates whether a string is a valid repository ID.
   *
   * <p>The rules are: <em>{@value #OWNER_RULE}</em>
   *
   * @param owner the repository ID string to test.
   */
  public static String validateOwner(String owner) {
    if (isValidOwner(owner)) {
      return owner;
    }
    throw new IllegalArgumentException(OWNER_MESSAGE + " - but was: " + owner);
  }

  /**
   * Just checks whether a string is a valid repository ID, but doesn't throw an exception.
   *
   * @see #validateRepo(String)
   */
  public static boolean isValidRepo(String repo) {
    Objects.requireNonNull(repo, "repo must not be null");
    Matcher matcher = Validation.REPO_PATTERN.matcher(repo);
    return matcher.matches() && !INVALID_REPO_IDS.contains(repo);
  }

  /**
   * Validates whether a string is a valid repository ID.
   *
   * <p>The rules are: <em>{@value #REPO_RULE}</em>
   *
   * @param repo the repository ID string to test.
   */
  public static String validateRepo(String repo) {
    if (isValidRepo(repo)) {
      return repo;
    }
    throw new IllegalArgumentException(REPO_MESSAGE + " - but was: " + repo);
  }

  /**
   * Just checks whether a string is a valid reference-name, but doesn't throw an exception.
   *
   * @see #validateReferenceName(String)
   */
  public static boolean isValidReferenceName(String referenceName) {
    Objects.requireNonNull(referenceName, "referenceName must not be null");
    Matcher matcher = Validation.REF_NAME_PATTERN.matcher(referenceName);
    return matcher.matches();
  }

  /**
   * Just checks whether a string is a valid hash, but doesn't throw an exception.
   *
   * @see #validateHash(String)
   */
  public static boolean isValidHash(String hash) {
    Objects.requireNonNull(hash, "hash must not be null");
    Matcher matcher = Validation.HASH_PATTERN.matcher(hash);
    return matcher.matches();
  }

  /**
   * Just checks whether a string is a valid reference-name (as per {@link
   * #isValidReferenceName(String)}) or a valid hash (as per {@link #isValidHash(String)}).
   */
  public static boolean isValidReferenceNameOrHash(String ref) {
    Objects.requireNonNull(ref, "reference (name or hash) must not be null");
    Matcher matcher = Validation.REF_NAME_OR_HASH_PATTERN.matcher(ref);
    return matcher.matches();
  }

  /**
   * Validates whether a string is a valid reference-name.
   *
   * <p>The rules are: <em>{@value #REF_RULE}</em>
   *
   * @param referenceName the reference name string to test.
   */
  public static String validateReferenceName(String referenceName) {
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
  public static String validateHash(String referenceName) {
    if (isValidHash(referenceName)) {
      return referenceName;
    }
    throw new IllegalArgumentException(HASH_MESSAGE + " - but was: " + referenceName);
  }

  /**
   * Validates whether a string is a valid reference name or hash.
   *
   * <p>See {@link #validateReferenceName(String)} and {@link #validateHash(String)} for the rules.
   *
   * @param ref the reference name string to test.
   */
  public static String validateReferenceNameOrHash(String ref) {
    if (isValidReferenceNameOrHash(ref)) {
      return ref;
    }
    throw new IllegalArgumentException(REF_NAME_OR_HASH_MESSAGE + " - but was: " + ref);
  }
}
