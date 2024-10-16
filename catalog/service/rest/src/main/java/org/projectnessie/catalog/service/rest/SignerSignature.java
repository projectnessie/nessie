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

import static com.google.common.hash.Hashing.hmacSha256;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.hash.Hasher;
import jakarta.validation.constraints.NotNull;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.projectnessie.catalog.service.objtypes.SignerKey;
import org.projectnessie.nessie.immutables.NessieImmutable;

@NessieImmutable
@JsonSerialize(as = ImmutableSignerSignature.class)
@JsonDeserialize(as = ImmutableSignerSignature.class)
public abstract class SignerSignature {

  public abstract String prefix();

  public abstract String identifier();

  public abstract String warehouseLocation();

  public abstract List<String> writeLocations();

  public abstract List<String> readLocations();

  public abstract long expirationTimestamp();

  @SuppressWarnings("UnstableApiUsage")
  public String sign(SignerKey signerKey) {
    Hasher hasher = hmacSha256(signerKey.secretKeySpec()).newHasher();
    hasher.putLong(expirationTimestamp());
    hasher.putString("prefix=" + prefix(), UTF_8);
    hasher.putString("identifier=" + identifier(), UTF_8);
    hasher.putString("b=" + warehouseLocation(), UTF_8);
    for (String writeLocation : writeLocations()) {
      hasher.putString("w=" + writeLocation, UTF_8);
    }
    for (String readLocation : readLocations()) {
      hasher.putString("r=" + readLocation, UTF_8);
    }
    return hasher.hash().toString();
  }

  public String toPathParam(SignerKey signerKey) {
    String signature = sign(signerKey);
    return SignerParams.builder()
        .keyName(signerKey.name())
        .signature(signature)
        .signerSignature(this)
        .build()
        .toPathParam();
  }

  public Optional<String> verify(
      SignerKey signerKey, @NotNull String signature, @NotNull Instant now) {
    if (signerKey == null) {
      return Optional.of("Could not find signingKey");
    }

    String computed = sign(signerKey);

    if (!computed.equals(signature)) {
      return Optional.of("Got invalid signature");
    }

    if (expirationTimestamp() <= now.getEpochSecond()) {
      return Optional.of("Got expired signature");
    }

    return Optional.empty();
  }

  static ImmutableSignerSignature.Builder builder() {
    return ImmutableSignerSignature.builder();
  }
}
