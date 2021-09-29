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

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.projectnessie.model.Validation.validateHash;
import static org.projectnessie.model.Validation.validateReferenceName;
import static org.projectnessie.model.Validation.validateReferenceNameOrHash;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class TestValidation {

  @ParameterizedTest
  @ValueSource(strings = {"a", "a_b-", "a_-c", "abc/def"})
  void validRefNames(String referenceName) {
    validateReferenceName(referenceName);
    validateReferenceNameOrHash(referenceName);
    Branch.of(referenceName, null);
    Tag.of(referenceName, null);
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "abc/", ".foo", "abc/def/../blah", "abc/de..blah", "abc/de@{blah"})
  void invalidRefNames(String referenceName) {
    assertAll(
        () ->
            assertEquals(
                Validation.REF_NAME_MESSAGE + " - but was: " + referenceName,
                assertThrows(
                        IllegalArgumentException.class, () -> validateReferenceName(referenceName))
                    .getMessage()),
        () ->
            assertEquals(
                Validation.REF_NAME_OR_HASH_MESSAGE + " - but was: " + referenceName,
                assertThrows(
                        IllegalArgumentException.class,
                        () -> validateReferenceNameOrHash(referenceName))
                    .getMessage()),
        () ->
            assertEquals(
                Validation.REF_NAME_MESSAGE + " - but was: " + referenceName,
                assertThrows(IllegalArgumentException.class, () -> Branch.of(referenceName, null))
                    .getMessage()),
        () ->
            assertEquals(
                Validation.REF_NAME_MESSAGE + " - but was: " + referenceName,
                assertThrows(IllegalArgumentException.class, () -> Tag.of(referenceName, null))
                    .getMessage()));
  }

  @Test
  void nullParam() {
    assertAll(
        () -> assertThrows(NullPointerException.class, () -> validateReferenceName(null)),
        () -> assertThrows(NullPointerException.class, () -> Branch.of(null, null)),
        () -> assertThrows(NullPointerException.class, () -> Tag.of(null, null)));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "1122334455667788990011223344556677889900112233445566778899001122",
        "abcDEF4242424242424242424242BEEF00DEAD42112233445566778899001122",
        "0011223344556677",
        "11223344556677889900"
      })
  void validHashes(String hash) {
    validateHash(hash);
    validateReferenceNameOrHash(hash);
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "abc/", ".foo", "abc/def/../blah", "abc/de..blah", "abc/de@{blah"})
  void invalidHashes(String hash) {
    String referenceName = "thisIsAValidName";
    assertAll(
        () ->
            assertEquals(
                Validation.HASH_MESSAGE + " - but was: " + hash,
                assertThrows(IllegalArgumentException.class, () -> validateHash(hash))
                    .getMessage()),
        () ->
            assertEquals(
                Validation.HASH_MESSAGE + " - but was: " + hash,
                assertThrows(IllegalArgumentException.class, () -> Branch.of(referenceName, hash))
                    .getMessage()),
        () ->
            assertEquals(
                Validation.HASH_MESSAGE + " - but was: " + hash,
                assertThrows(IllegalArgumentException.class, () -> Tag.of(referenceName, hash))
                    .getMessage()));
  }

  @ParameterizedTest
  @CsvSource({
    "a,112233445566778899001122abcDEF4242424242424242424242BEEF00DEAD42",
    "a,1122334455667788990011221122334455667788990011223344556677889900",
    "a_b-,112233445566778899001122abcDEF4242424242424242424242BEEF00DEAD42",
    "a_b-,1122334455667788990011221122334455667788990011223344556677889900",
    "a_-c,1122334455667788",
    "a_-c,112233445566778899001122",
    "abc/def,1122334455667788990011223344556677889900"
  })
  void validNamesAndHashes(String referenceName, String hash) {
    Branch.of(referenceName, hash);
    Tag.of(referenceName, hash);
  }

  @ParameterizedTest
  @CsvSource({
    "a,abcDEF4242424242424242424242BEEF00DEADxy",
    "a,11",
    "a_b-,meep",
    "a_b-,0",
    "a_-c,##",
    "a_-c,123",
    "abc/def,nonono"
  })
  void validNamesAndInvalidHashes(String referenceName, String hash) {
    assertAll(
        () ->
            assertEquals(
                Validation.HASH_MESSAGE + " - but was: " + hash,
                assertThrows(IllegalArgumentException.class, () -> Branch.of(referenceName, hash))
                    .getMessage()),
        () ->
            assertEquals(
                Validation.HASH_MESSAGE + " - but was: " + hash,
                assertThrows(IllegalArgumentException.class, () -> Tag.of(referenceName, hash))
                    .getMessage()));
  }
}
