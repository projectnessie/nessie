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
package org.projectnessie.model;

import static org.assertj.core.api.Assumptions.assumeThat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.function.Function;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.projectnessie.error.ErrorCode;
import org.projectnessie.error.NessieError;
import org.projectnessie.model.Conflict.ConflictType;
import org.projectnessie.model.MergeResponse.ContentKeyConflict;
import org.projectnessie.model.MergeResponse.ContentKeyDetails;

@ExtendWith(SoftAssertionsExtension.class)
public class TestEnums {
  @InjectSoftAssertions protected SoftAssertions soft;

  @SuppressWarnings("unused")
  abstract class AbstractEnum<E extends Enum<E>> {
    final Function<String, E> parse;
    final E unknownValue;
    final Function<ObjectMapper, E> unknownJsonTest;

    AbstractEnum(
        Function<String, E> parse, E unknownValue, Function<ObjectMapper, E> unknownJsonTest) {
      this.parse = parse;
      this.unknownValue = unknownValue;
      this.unknownJsonTest = unknownJsonTest;
    }

    @Test
    public void testUnknown() {
      if (unknownValue == null) {
        soft.assertThatIllegalArgumentException().isThrownBy(() -> parse.apply("FOO_BAR"));
      } else {
        soft.assertThat(parse.apply("FOO_BAR")).isSameAs(unknownValue);
      }
    }

    @Test
    public void testUnknownJson() {
      assumeThat(unknownJsonTest).isNotNull();

      ObjectMapper mapper = new ObjectMapper();
      if (unknownValue == null) {
        soft.assertThatThrownBy(() -> unknownJsonTest.apply(mapper))
            .hasCauseInstanceOf(JsonMappingException.class)
            .hasMessageContaining(".FOO_BAR ");
      } else {
        soft.assertThat(unknownJsonTest.apply(mapper)).isSameAs(unknownValue);
      }
    }

    @Test
    public void testNull() {
      soft.assertThat(parse.apply(null)).isNull();
    }

    void conversion(E value) {
      soft.assertThat(parse.apply(value.name())).isSameAs(value);
    }
  }

  @Nested
  public class TestErrorCode extends AbstractEnum<ErrorCode> {
    TestErrorCode() {
      super(
          ErrorCode::parse,
          ErrorCode.UNKNOWN,
          mapper -> {
            try {
              return mapper
                  .readValue(
                      "{"
                          + "\"status\": 42,"
                          + "\"reason\": \"reason\","
                          + "\"errorCode\": \"FOO_BAR\""
                          + "}",
                      NessieError.class)
                  .getErrorCode();
            } catch (JsonProcessingException e) {
              throw new RuntimeException(e);
            }
          });
    }

    @ParameterizedTest
    @EnumSource(value = ErrorCode.class)
    public void testConversion(ErrorCode errorCode) {
      super.conversion(errorCode);
    }
  }

  @Nested
  public class TestFetchOption extends AbstractEnum<FetchOption> {
    TestFetchOption() {
      super(FetchOption::parse, FetchOption.ALL, null);
    }

    @ParameterizedTest
    @EnumSource(value = FetchOption.class)
    public void testConversion(FetchOption fetchOption) {
      super.conversion(fetchOption);
    }
  }

  @Nested
  public class TestConflictType extends AbstractEnum<ConflictType> {
    TestConflictType() {
      super(
          ConflictType::parse,
          ConflictType.UNKNOWN,
          mapper -> {
            try {
              return mapper
                  .readValue(
                      "{"
                          + "\"key\": { \"elements\": [\"a\"] },"
                          + "\"message\": \"reason\","
                          + "\"conflictType\": \"FOO_BAR\""
                          + "}",
                      Conflict.class)
                  .conflictType();
            } catch (JsonProcessingException e) {
              throw new RuntimeException(e);
            }
          });
    }

    @ParameterizedTest
    @EnumSource(value = ConflictType.class)
    public void testConversion(ConflictType conflictType) {
      conversion(conflictType);
    }
  }

  @SuppressWarnings("deprecation")
  @Nested
  public class TestContentKeyConflict extends AbstractEnum<ContentKeyConflict> {
    TestContentKeyConflict() {
      super(
          ContentKeyConflict::parse,
          ContentKeyConflict.UNRESOLVABLE,
          mapper -> {
            try {
              return mapper
                  .readValue(
                      "{"
                          + "\"key\": { \"elements\": [\"a\"] },"
                          + "\"mergeBehavior\": \"NORMAL\","
                          + "\"conflictType\": \"FOO_BAR\""
                          + "}",
                      ContentKeyDetails.class)
                  .getConflictType();
            } catch (JsonProcessingException e) {
              throw new RuntimeException(e);
            }
          });
    }

    @ParameterizedTest
    @EnumSource(value = ContentKeyConflict.class)
    public void testConversion(ContentKeyConflict contentKeyConflict) {
      conversion(contentKeyConflict);
    }
  }
}
