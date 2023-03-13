/*
 * Copyright (C) 2023 Dremio
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
package org.projectnessie.api.v2.params;

import static org.projectnessie.api.v2.params.ReferenceResolver.resolveReferencePathElement;

import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.projectnessie.model.Detached;
import org.projectnessie.model.Reference;
import org.projectnessie.model.Reference.ReferenceType;

@ExtendWith(SoftAssertionsExtension.class)
public class TestParsedReference {
  @InjectSoftAssertions private SoftAssertions soft;

  @ParameterizedTest
  @CsvSource({
    "a@11223344,a,11223344",
    "a,a,",
    "a/b,a/b,",
    "a/b@11223344,a/b,11223344",
    "a@,a,",
    "a/b@,a/b,",
  })
  void fromPathString(String pathParameter, String expectedName, String expectedHash) {
    for (ReferenceType type : ReferenceType.values()) {
      soft.assertThat(resolveReferencePathElement(pathParameter, type))
          .extracting(ParsedReference::type, ParsedReference::name, ParsedReference::hash)
          .containsExactly(type, expectedName, expectedHash);

      soft.assertThat(resolveReferencePathElement(pathParameter, type))
          .extracting(ParsedReference::toReference)
          .extracting(Reference::getType, Reference::getName, Reference::getHash)
          .containsExactly(type, expectedName, expectedHash);
    }

    soft.assertThat(resolveReferencePathElement(pathParameter, null))
        .extracting(ParsedReference::type, ParsedReference::name, ParsedReference::hash)
        .containsExactly(null, expectedName, expectedHash);

    soft.assertThatIllegalArgumentException()
        .isThrownBy(() -> resolveReferencePathElement(pathParameter, null).toReference())
        .withMessage("Cannot convert a name to a typed reference without a type");
  }

  @Test
  void fromPathStringDetached() {
    soft.assertThat(resolveReferencePathElement("@11223344", ReferenceType.BRANCH))
        .extracting(ParsedReference::hash)
        .isEqualTo("11223344");

    soft.assertThat(resolveReferencePathElement("@11223344", ReferenceType.BRANCH))
        .extracting(ParsedReference::toReference)
        .isInstanceOf(Detached.class)
        .extracting(Reference::getHash)
        .isEqualTo("11223344");

    soft.assertThat(resolveReferencePathElement("@11223344", null))
        .extracting(ParsedReference::hash)
        .isEqualTo("11223344");

    soft.assertThat(resolveReferencePathElement("@11223344", null))
        .extracting(ParsedReference::toReference)
        .isInstanceOf(Detached.class)
        .extracting(Reference::getHash)
        .isEqualTo("11223344");
  }
}
