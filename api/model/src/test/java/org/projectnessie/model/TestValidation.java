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

import static org.projectnessie.model.Validation.FORBIDDEN_REF_NAME_MESSAGE;
import static org.projectnessie.model.Validation.HASH_MESSAGE;
import static org.projectnessie.model.Validation.HASH_OR_RELATIVE_COMMIT_SPEC_MESSAGE;
import static org.projectnessie.model.Validation.HASH_OR_RELATIVE_COMMIT_SPEC_PATTERN;
import static org.projectnessie.model.Validation.REF_NAME_MESSAGE;
import static org.projectnessie.model.Validation.REF_NAME_PATH_PATTERN;
import static org.projectnessie.model.Validation.RELATIVE_COMMIT_SPEC_PART_PATTERN;
import static org.projectnessie.model.Validation.isForbiddenReferenceName;
import static org.projectnessie.model.Validation.validateForbiddenReferenceName;
import static org.projectnessie.model.Validation.validateHash;
import static org.projectnessie.model.Validation.validateHashOrRelativeSpec;
import static org.projectnessie.model.Validation.validateReferenceName;
import static org.projectnessie.model.Validation.validateReferenceNameOrHash;

import java.util.regex.Matcher;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

@ExtendWith(SoftAssertionsExtension.class)
class TestValidation {
  @InjectSoftAssertions protected SoftAssertions soft;

  @ParameterizedTest
  @ValueSource(strings = {"a", "a_b-", "a_-c", "abc/def"})
  void validRefNames(String referenceName) {
    soft.assertThatCode(() -> validateReferenceName(referenceName)).doesNotThrowAnyException();
    soft.assertThatCode(() -> validateReferenceNameOrHash(referenceName))
        .doesNotThrowAnyException();
    Branch.of(referenceName, null);
    Tag.of(referenceName, null);

    soft.assertThat(isForbiddenReferenceName(referenceName)).isFalse();
    soft.assertThatCode(() -> validateForbiddenReferenceName(referenceName))
        .doesNotThrowAnyException();
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "abc/", ".foo", "abc/def/../blah", "abc/de..blah", "abc/de@{blah"})
  void invalidRefNames(String referenceName) {
    soft.assertThatIllegalArgumentException()
        .isThrownBy(() -> validateReferenceName(referenceName))
        .withMessage(REF_NAME_MESSAGE + " - but was: " + referenceName);
    soft.assertThatIllegalArgumentException()
        .isThrownBy(() -> validateReferenceNameOrHash(referenceName))
        .withMessage(Validation.REF_NAME_OR_HASH_MESSAGE + " - but was: " + referenceName);
    soft.assertThatIllegalArgumentException()
        .isThrownBy(() -> Branch.of(referenceName, null))
        .withMessage(REF_NAME_MESSAGE + " - but was: " + referenceName);
    soft.assertThatIllegalArgumentException()
        .isThrownBy(() -> Tag.of(referenceName, null))
        .withMessage(REF_NAME_MESSAGE + " - but was: " + referenceName);
  }

  @Test
  void nullParam() {
    soft.assertThatNullPointerException().isThrownBy(() -> validateReferenceName(null));
    soft.assertThatNullPointerException().isThrownBy(() -> Branch.of(null, null));
    soft.assertThatNullPointerException().isThrownBy(() -> Tag.of(null, null));
  }

  @ParameterizedTest
  @ValueSource(strings = {"DETACHED", "HEAD", "detached", "head", "dEtAcHeD", "hEaD"})
  // Note: hashes validated in validHashes()
  void forbiddenReferenceNames(String refName) {
    soft.assertThat(isForbiddenReferenceName(refName)).isTrue();
    soft.assertThatThrownBy(() -> validateForbiddenReferenceName(refName))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith(FORBIDDEN_REF_NAME_MESSAGE);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "1122334455667788990011223344556677889900112233445566778899001122",
        "abcDEF4242424242424242424242BEEF00DEAD42112233445566778899001122",
        "0011223344556677",
        "11223344556677889900",
        "cafebabedeadbeef",
        "CAFEBABEDEADBEEF",
        "caffee20",
        "20caffee",
        "20caff22"
      })
  void validHashes(String hash) {
    soft.assertThatCode(() -> validateHash(hash)).doesNotThrowAnyException();
    soft.assertThatCode(() -> validateReferenceNameOrHash(hash)).doesNotThrowAnyException();
    soft.assertThat(isForbiddenReferenceName(hash)).isTrue();
    soft.assertThatThrownBy(() -> validateForbiddenReferenceName(hash))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith(FORBIDDEN_REF_NAME_MESSAGE);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "1122334455667788990011223344556677889900112233445566778899001122",
        "abcDEF4242424242424242424242BEEF00DEAD42112233445566778899001122",
        "0011223344556677",
        "11223344556677889900",
        "cafebabedeadbeef",
        "CAFEBABEDEADBEEF",
        "caffee20",
        "20caffee",
        "20caff22",
        "~2",
        "^3",
        "*1234567890",
        "*2023-07-27T16:14:23.123456789Z",
        "*2023-07-27T16:14:23.123456Z",
        "*2023-07-27T16:14:23.123Z",
        "*2023-07-27T16:14:23Z",
        "1122334455667788990011223344556677889900112233445566778899001122~2",
        "11223344556677889900^3",
        "cafebabedeadbeef*1234567890",
        "cafebabedeadbeef*2023-07-27T16:14:23.123456Z",
      })
  void validHashOrRelativeSpecs(String hash) {
    soft.assertThatCode(() -> validateHashOrRelativeSpec(hash)).doesNotThrowAnyException();
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "abc/", ".foo", "abc/def/../blah", "abc/de..blah", "abc/de@{blah"})
  void invalidHashes(String hash) {
    String referenceName = "thisIsAValidName";
    soft.assertThatIllegalArgumentException()
        .isThrownBy(() -> validateHash(hash))
        .withMessage(HASH_MESSAGE + " - but was: " + hash);
    soft.assertThatIllegalArgumentException()
        .isThrownBy(() -> validateHashOrRelativeSpec(hash))
        .withMessage(HASH_OR_RELATIVE_COMMIT_SPEC_MESSAGE + " - but was: " + hash);
    soft.assertThatIllegalArgumentException()
        .isThrownBy(() -> Branch.of(referenceName, hash))
        .withMessage(HASH_OR_RELATIVE_COMMIT_SPEC_MESSAGE + " - but was: " + hash);
    soft.assertThatIllegalArgumentException()
        .isThrownBy(() -> Tag.of(referenceName, hash))
        .withMessage(HASH_OR_RELATIVE_COMMIT_SPEC_MESSAGE + " - but was: " + hash);
  }

  @ParameterizedTest
  @CsvSource({
    "a,112233445566778899001122abcDEF4242424242424242424242BEEF00DEAD42",
    "a,1122334455667788990011221122334455667788990011223344556677889900",
    "a_b-,112233445566778899001122abcDEF4242424242424242424242BEEF00DEAD42",
    "a_b-,1122334455667788990011221122334455667788990011223344556677889900",
    "a_-c,1122334455667788",
    "a_-c,112233445566778899001122",
    "abc/def,1122334455667788990011223344556677889900",
    "coffee20,1122334455667788990011223344556677889900",
    "coffee2go,1122334455667788990011223344556677889900"
  })
  void validNamesAndHashes(String referenceName, String hash) {
    soft.assertThatCode(() -> Branch.of(referenceName, hash)).doesNotThrowAnyException();
    soft.assertThatCode(() -> Tag.of(referenceName, hash)).doesNotThrowAnyException();
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
    soft.assertThatIllegalArgumentException()
        .isThrownBy(() -> Branch.of(referenceName, hash))
        .withMessage(HASH_OR_RELATIVE_COMMIT_SPEC_MESSAGE + " - but was: " + hash);
    soft.assertThatIllegalArgumentException()
        .isThrownBy(() -> Tag.of(referenceName, hash))
        .withMessage(HASH_OR_RELATIVE_COMMIT_SPEC_MESSAGE + " - but was: " + hash);
  }

  @ParameterizedTest
  // with and without hash
  @CsvSource({"12345678*11111~22222^33333,12345678", "*11111~22222^33333,"})
  void validHashTimestampParentPatterns_3_parts(String pattern, String hash) {
    Matcher outer = HASH_OR_RELATIVE_COMMIT_SPEC_PATTERN.matcher(pattern);

    soft.assertThat(outer.matches()).isTrue();
    soft.assertThat(outer.group(1)).isEqualTo(hash);

    Matcher matcher = RELATIVE_COMMIT_SPEC_PART_PATTERN.matcher(outer.group(2));
    soft.assertThat(matcher.find()).isTrue();
    soft.assertThat(matcher.group(1)).isEqualTo("*");
    soft.assertThat(matcher.group(2)).isEqualTo("11111");

    soft.assertThat(matcher.find()).isTrue();
    soft.assertThat(matcher.group(1)).isEqualTo("~");
    soft.assertThat(matcher.group(2)).isEqualTo("22222");

    soft.assertThat(matcher.find()).isTrue();
    soft.assertThat(matcher.group(1)).isEqualTo("^");
    soft.assertThat(matcher.group(2)).isEqualTo("33333");

    soft.assertThat(matcher.find()).isFalse();

    soft.assertThat(outer.find()).isFalse();
  }

  @ParameterizedTest
  // with and without hash
  @CsvSource({"12345678*11111,12345678", "*11111,"})
  void validHashTimestampParentPatterns_1_part(String pattern, String hash) {
    Matcher outer = HASH_OR_RELATIVE_COMMIT_SPEC_PATTERN.matcher(pattern);

    soft.assertThat(outer.matches()).isTrue();
    soft.assertThat(outer.group(1)).isEqualTo(hash);

    Matcher matcher = RELATIVE_COMMIT_SPEC_PART_PATTERN.matcher(outer.group(2));
    soft.assertThat(matcher.find()).isTrue();
    soft.assertThat(matcher.group(1)).isEqualTo("*");
    soft.assertThat(matcher.group(2)).isEqualTo("11111");

    soft.assertThat(matcher.find()).isFalse();

    soft.assertThat(outer.find()).isFalse();
  }

  @ParameterizedTest
  // with and without hash
  @CsvSource({
    "12345678,12345678",
    "2e1cfa82b035c26cbbbdae632cea070514eb8b773f616aaeaf668e2f0be8f10d,2e1cfa82b035c26cbbbdae632cea070514eb8b773f616aaeaf668e2f0be8f10d",
    ","
  })
  void validHashTimestampParentPatterns_no_parts(String pattern, String hash) {
    Matcher outer = HASH_OR_RELATIVE_COMMIT_SPEC_PATTERN.matcher(pattern != null ? pattern : "");

    soft.assertThat(outer.matches()).isTrue();
    soft.assertThat(outer.group(1)).isEqualTo(hash);

    Matcher matcher = RELATIVE_COMMIT_SPEC_PART_PATTERN.matcher(outer.group(2));

    soft.assertThat(matcher.find()).isFalse();

    soft.assertThat(outer.find()).isFalse();
  }

  @ParameterizedTest
  @CsvSource({
    "testFrom@2e1cfa82b035c26cbbbdae632cea070514eb8b773f616aaeaf668e2f0be8f10d,testFrom,2e1cfa82b035c26cbbbdae632cea070514eb8b773f616aaeaf668e2f0be8f10d,",
    "@6dd38434e4520966085a2f428b6a9803358dd31997661e44a7038eb66018a5f1,,6dd38434e4520966085a2f428b6a9803358dd31997661e44a7038eb66018a5f1,",
    "test~10,test,,~10",
    "test@~1,test,,~1", // additional '@' is legit on REST path names, but not recommended!
    "test~1,test,,~1",
    "~10,,,~10",
    "~1,,,~1",
    "test@6dd38434e4520966085a2f428b6a9803358dd31997661e44a7038eb66018a5f1~10,test,6dd38434e4520966085a2f428b6a9803358dd31997661e44a7038eb66018a5f1,~10",
    "test@6dd38434e4520966085a2f428b6a9803358dd31997661e44a7038eb66018a5f1~1,test,6dd38434e4520966085a2f428b6a9803358dd31997661e44a7038eb66018a5f1,~1",
    "@6dd38434e4520966085a2f428b6a9803358dd31997661e44a7038eb66018a5f1~10,,6dd38434e4520966085a2f428b6a9803358dd31997661e44a7038eb66018a5f1,~10",
    "@6dd38434e4520966085a2f428b6a9803358dd31997661e44a7038eb66018a5f1~1,,6dd38434e4520966085a2f428b6a9803358dd31997661e44a7038eb66018a5f1,~1",
  })
  void pathRefName(String rest, String ref, String hashOnRef, String relativeSpec) {
    Matcher matcher = REF_NAME_PATH_PATTERN.matcher(rest);
    soft.assertThat(matcher.matches()).isTrue();

    soft.assertThat(matcher.group(1)).isEqualTo(ref);
    soft.assertThat(matcher.group(2)).isEqualTo(hashOnRef);
    soft.assertThat(matcher.group(3)).isEqualTo(relativeSpec);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "abcDEF4242424242424242424242BEEF00DEAD42112233445566778899001122",
        "1P12",
        "PT10D",
        "2011-12-03T10:15:30",
        "2011-12-03 10:15:30"
      })
  void invalidDefaultCutOffPolicy(String cutOffPolicy) {
    soft.assertThatIllegalArgumentException()
        .isThrownBy(() -> Validation.validateDefaultCutOffPolicy(cutOffPolicy))
        .withMessageContaining("Failed to parse default-cutoff-value");
  }

  @ParameterizedTest
  @ValueSource(
      strings = {"123", "P10D", "p20D", "p10d", "PT20.3S", "P2DT3H4M", "2011-12-03T10:15:30Z"})
  void validateDefaultCutOffPolicy(String cutOffPolicy) {
    soft.assertThatCode(() -> Validation.validateDefaultCutOffPolicy(cutOffPolicy))
        .doesNotThrowAnyException();
  }
}
