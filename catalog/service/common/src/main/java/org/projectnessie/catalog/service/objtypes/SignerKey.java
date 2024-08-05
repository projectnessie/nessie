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
package org.projectnessie.catalog.service.objtypes;

import static com.google.common.base.Preconditions.checkState;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.time.Instant;
import javax.crypto.spec.SecretKeySpec;
import org.immutables.value.Value;
import org.projectnessie.nessie.immutables.NessieImmutable;

@NessieImmutable
@JsonSerialize(as = ImmutableSignerKey.class)
@JsonDeserialize(as = ImmutableSignerKey.class)
public interface SignerKey {
  String name();

  byte[] secretKey();

  Instant creationTime();

  Instant rotationTime();

  Instant expirationTime();

  @JsonIgnore
  @Value.Lazy
  default SecretKeySpec secretKeySpec() {
    return new SecretKeySpec(secretKey(), "hmacSha256");
  }

  @Value.Check
  default void check() {
    checkState(!name().isEmpty(), "Key name must not be empty");
    checkState(secretKey().length >= 32, "Secret key too short");
    checkState(
        creationTime().compareTo(rotationTime()) < 0, "creationTime must be before rotationTime");
    checkState(
        rotationTime().compareTo(expirationTime()) < 0,
        "rotationTime must be before expirationTime");
  }

  static ImmutableSignerKey.Builder builder() {
    return ImmutableSignerKey.builder();
  }
}
