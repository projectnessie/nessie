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
package org.projectnessie.catalog.secrets.cache;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.LongSupplier;
import org.immutables.value.Value;

@Value.Immutable
public interface SecretsCacheConfig {

  long maxElements();

  OptionalLong ttlMillis();

  Optional<MeterRegistry> meterRegistry();

  @Value.Default
  default LongSupplier clockNanos() {
    return System::nanoTime;
  }

  static Builder builder() {
    return ImmutableSecretsCacheConfig.builder();
  }

  interface Builder {
    @CanIgnoreReturnValue
    Builder maxElements(long maxElements);

    @CanIgnoreReturnValue
    Builder ttlMillis(long ttlMillis);

    @CanIgnoreReturnValue
    Builder meterRegistry(MeterRegistry meterRegistry);

    @CanIgnoreReturnValue
    Builder clockNanos(LongSupplier clockNanos);

    SecretsCacheConfig build();
  }
}
