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
package org.projectnessie.services.rest;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import javax.ws.rs.ext.ParamConverter;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class InstantParamConverterProviderTest {

  private final ParamConverter<Instant> converter =
      new InstantParamConverterProvider().getConverter(Instant.class, null, null);

  @Test
  public void testNulls() {
    assertThat(converter.fromString(null)).isNull();
    assertThat(converter.toString(null)).isNull();
  }

  @Test
  public void testInvalid() {
    Assertions.assertThatThrownBy(() -> converter.fromString("invalid"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("'invalid' could not be parsed to an Instant in ISO-8601 format");
  }

  @Test
  public void testValid() {
    Instant now = Instant.now();
    assertThat(converter.fromString(now.toString())).isEqualTo(now);
    assertThat(converter.toString(now)).isEqualTo(now.toString());
  }
}
