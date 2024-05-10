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
package org.projectnessie.catalog.files.adls;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.time.Duration;
import java.util.Map;
import org.immutables.value.Value;

@Value.Immutable
public interface AdlsProgrammaticOptions extends AdlsOptions<AdlsFileSystemOptions> {
  @Override
  Map<String, AdlsFileSystemOptions> fileSystems();

  static Builder builder() {
    return ImmutableAdlsProgrammaticOptions.builder();
  }

  interface Builder {
    @CanIgnoreReturnValue
    Builder writeBlockSize(long writeBlockSize);

    @CanIgnoreReturnValue
    Builder putConfigurationOptions(String key, String value);

    @CanIgnoreReturnValue
    Builder putConfigurationOptions(Map.Entry<String, ? extends String> entry);

    @CanIgnoreReturnValue
    Builder configurationOptions(Map<String, ? extends String> entries);

    @CanIgnoreReturnValue
    Builder putAllConfigurationOptions(Map<String, ? extends String> entries);

    @CanIgnoreReturnValue
    Builder putFileSystems(String key, AdlsFileSystemOptions value);

    @CanIgnoreReturnValue
    Builder putFileSystems(Map.Entry<String, ? extends AdlsFileSystemOptions> entry);

    @CanIgnoreReturnValue
    Builder fileSystems(Map<String, ? extends AdlsFileSystemOptions> entries);

    @CanIgnoreReturnValue
    Builder putAllFileSystems(Map<String, ? extends AdlsFileSystemOptions> entries);

    @CanIgnoreReturnValue
    Builder accountNameRef(String accountNameRef);

    @CanIgnoreReturnValue
    Builder accountKeyRef(String accountKeyRef);

    @CanIgnoreReturnValue
    Builder sasTokenRef(String sasTokenRef);

    @CanIgnoreReturnValue
    Builder endpoint(String endpoint);

    @CanIgnoreReturnValue
    Builder retryPolicy(AdlsRetryStrategy retryPolicy);

    @CanIgnoreReturnValue
    Builder maxRetries(int maxRetries);

    @CanIgnoreReturnValue
    Builder tryTimeout(Duration tryTimeout);

    @CanIgnoreReturnValue
    Builder retryDelay(Duration retryDelay);

    @CanIgnoreReturnValue
    Builder maxRetryDelay(Duration maxRetryDelay);

    AdlsProgrammaticOptions build();
  }

  @Value.Immutable
  interface AdlsPerFileSystemOptions extends AdlsFileSystemOptions {

    static Builder builder() {
      return ImmutableAdlsPerFileSystemOptions.builder();
    }

    interface Builder {
      @CanIgnoreReturnValue
      Builder from(AdlsFileSystemOptions instance);

      @CanIgnoreReturnValue
      Builder accountNameRef(String accountNameRef);

      @CanIgnoreReturnValue
      Builder accountKeyRef(String accountKeyRef);

      @CanIgnoreReturnValue
      Builder sasTokenRef(String sasTokenRef);

      @CanIgnoreReturnValue
      Builder endpoint(String endpoint);

      @CanIgnoreReturnValue
      Builder externalEndpoint(String externalEndpoint);

      @CanIgnoreReturnValue
      Builder retryPolicy(AdlsRetryStrategy retryPolicy);

      @CanIgnoreReturnValue
      Builder maxRetries(int maxRetries);

      @CanIgnoreReturnValue
      Builder tryTimeout(Duration tryTimeout);

      @CanIgnoreReturnValue
      Builder retryDelay(Duration retryDelay);

      @CanIgnoreReturnValue
      Builder maxRetryDelay(Duration maxRetryDelay);

      AdlsPerFileSystemOptions build();
    }
  }
}
