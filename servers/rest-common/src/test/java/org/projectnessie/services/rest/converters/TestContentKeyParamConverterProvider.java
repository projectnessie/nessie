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
package org.projectnessie.services.rest.converters;

import static org.assertj.core.api.Assertions.assertThat;

import jakarta.ws.rs.ext.ParamConverter;
import org.junit.jupiter.api.Test;
import org.projectnessie.model.ContentKey;

public class TestContentKeyParamConverterProvider {

  private final ParamConverter<ContentKey> converter =
      new ContentKeyParamConverterProvider().getConverter(ContentKey.class, null, null);

  @Test
  public void testNulls() {
    assertThat(converter.fromString(null)).isNull();
    assertThat(converter.toString(null)).isNull();
  }

  @Test
  public void testValid() {
    ContentKey key = ContentKey.of("a.b.c");
    String name = "a\u001Db\u001Dc";
    assertThat(converter.fromString(name)).isEqualTo(key);
    assertThat(converter.toString(key)).isEqualTo(name);
  }
}
