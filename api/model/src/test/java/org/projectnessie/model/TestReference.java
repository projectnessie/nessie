/*
 * Copyright (C) 2022 Dremio
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

import static org.projectnessie.model.Validation.REF_NAME_PATH_ELEMENT_PATTERN;

import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

@ExtendWith(SoftAssertionsExtension.class)
class TestReference {
  @InjectSoftAssertions protected SoftAssertions soft;

  @ParameterizedTest
  @CsvSource({
    "a,11223344,a@11223344",
    "a,,a",
    "a/b,11223344,a/b@11223344",
    "a/b,,a/b@",
    ",11223344,@11223344",
    "test,~10,test~10",
    "test,*123456,test*123456",
    "test/path,~10,test/path@~10",
    "test,deadbeef,test@deadbeef",
  })
  void toPathString(String name, String hash, String expectedResult) {
    soft.assertThat(Reference.toPathString(name, hash)).isEqualTo(expectedResult);
    soft.assertThat(REF_NAME_PATH_ELEMENT_PATTERN.matcher(expectedResult).matches()).isTrue();
  }
}
