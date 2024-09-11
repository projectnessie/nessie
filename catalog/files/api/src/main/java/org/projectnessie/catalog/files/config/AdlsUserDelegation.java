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
package org.projectnessie.catalog.files.config;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.time.Duration;
import java.util.Optional;
import org.projectnessie.nessie.immutables.NessieImmutable;

@NessieImmutable
@JsonSerialize(as = ImmutableAdlsUserDelegation.class)
@JsonDeserialize(as = ImmutableAdlsUserDelegation.class)
public interface AdlsUserDelegation {

  /**
   * Enable short-lived user-delegation SAS tokens per file-system.
   *
   * <p>The current default is to not enable short-lived and scoped-down credentials, but the
   * default may change to enable in the future.
   */
  Optional<Boolean> enable();

  /**
   * Expiration time / validity duration of the user-delegation <em>key</em>, this key is
   * <em>not</em> passed to the client.
   *
   * <p>Defaults to 7 days minus 1 minute (the maximum), must be >= 1 second.
   */
  Optional<Duration> keyExpiry();

  /**
   * Expiration time / validity duration of the user-delegation <em>SAS token</em>, which
   * <em>is</em> sent to the client.
   *
   * <p>Defaults to 3 hours, must be >= 1 second.
   */
  Optional<Duration> sasExpiry();
}
