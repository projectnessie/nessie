/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.catalog.service.rest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.dataformat.smile.databind.SmileMapper;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Base64;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import org.projectnessie.nessie.immutables.NessieImmutable;

@NessieImmutable
@JsonSerialize(as = ImmutableSignerParams.class)
@JsonDeserialize(as = ImmutableSignerParams.class)
public abstract class SignerParams {

  private static final ObjectMapper SMILE_MAPPER = new SmileMapper().findAndRegisterModules();

  public abstract String keyName();

  public abstract String signature();

  public abstract SignerSignature signerSignature();

  public static SignerParams fromPathParam(String pathParam) {
    try {
      byte[] bin = Base64.getUrlDecoder().decode(pathParam);
      try (InputStream is = new GZIPInputStream(new ByteArrayInputStream(bin))) {
        return SMILE_MAPPER.readValue(is, SignerParams.class);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public String toPathParam() {
    try {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      try (OutputStream gzip = new GZIPOutputStream(out)) {
        SMILE_MAPPER.writeValue(gzip, this);
      }
      byte[] bin = out.toByteArray();
      return Base64.getUrlEncoder().withoutPadding().encodeToString(bin);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static ImmutableSignerParams.Builder builder() {
    return ImmutableSignerParams.builder();
  }
}
