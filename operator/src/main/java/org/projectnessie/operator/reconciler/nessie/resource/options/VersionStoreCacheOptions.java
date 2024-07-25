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
package org.projectnessie.operator.reconciler.nessie.resource.options;

import static org.projectnessie.operator.events.EventReason.InvalidVersionStoreConfig;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import io.fabric8.generator.annotation.Default;
import io.fabric8.generator.annotation.Nullable;
import io.fabric8.kubernetes.api.model.Quantity;
import io.sundr.builder.annotations.Buildable;
import java.math.BigDecimal;
import org.projectnessie.operator.exception.InvalidSpecException;

@Buildable(builderPackage = "io.fabric8.kubernetes.api.builder", editableEnabled = false)
@JsonInclude(Include.NON_NULL)
public record VersionStoreCacheOptions(
    @JsonPropertyDescription("Whether to enable the version store cache. The default is true.")
        @Default("true")
        Boolean enabled,
    @JsonPropertyDescription(
            "A fixed size for the cache. If this option is defined, other cache options are ignored.")
        @Nullable
        @jakarta.annotation.Nullable
        Quantity fixedSize,
    @JsonPropertyDescription(
            """
            The fraction of the available heap that the cache should use. \
            The default is 700m (70%). Must be > 0 and < 1000m. \
            Note: by default, Nessie servers are configured to use 70% of the available memory, \
            so the cache will by default use 70% of that.""")
        @Default("700m")
        Quantity heapFraction,
    @JsonPropertyDescription(
            """
            The minimum size of the cache. \
            This serves as a lower bound for the cache size. \
            The default is 64Mi. Cannot be less than 64Mi.""")
        @Default("64Mi")
        Quantity minSize,
    @JsonPropertyDescription(
            """
            The minimum amount of heap that should be kept free. \
            This servers as an upper bound for the cache size. \
            The default is 256Mi. Cannot be less than 64Mi.""")
        @Default("256Mi")
        Quantity minFreeHeap) {

  // These constants should be kept in sync
  // with org.projectnessie.versioned.storage.cache.CacheSizing

  public static final Quantity DEFAULT_HEAP_PERCENTAGE = Quantity.parse("700m");
  public static final Quantity DEFAULT_MIN_SIZE = Quantity.parse("64Mi");
  public static final Quantity DEFAULT_MIN_FREE_HEAP = Quantity.parse("256Mi");
  public static final Quantity MIN_SIZE = Quantity.parse("64Mi");

  public VersionStoreCacheOptions() {
    this(null, null, null, null, null);
  }

  public VersionStoreCacheOptions {
    enabled = enabled != null ? enabled : true;
    heapFraction = heapFraction != null ? heapFraction : DEFAULT_HEAP_PERCENTAGE;
    minSize = minSize != null ? minSize : DEFAULT_MIN_SIZE;
    minFreeHeap = minFreeHeap != null ? minFreeHeap : DEFAULT_MIN_FREE_HEAP;
  }

  public void validate() {
    if (enabled) {
      if (heapFraction.getNumericalAmount().compareTo(BigDecimal.ZERO) <= 0
          || heapFraction.getNumericalAmount().compareTo(BigDecimal.ONE) >= 0) {
        throw new InvalidSpecException(
            InvalidVersionStoreConfig,
            "Invalid cache configuration: spec.versionStore.cache.heapFraction must be > 0 and < 1");
      }
      if (minSize.getNumericalAmount().compareTo(MIN_SIZE.getNumericalAmount()) < 0) {
        throw new InvalidSpecException(
            InvalidVersionStoreConfig,
            "Invalid cache configuration: spec.versionStore.cache.minSize must be >= 64Mi");
      }
      if (minFreeHeap.getNumericalAmount().compareTo(MIN_SIZE.getNumericalAmount()) < 0) {
        throw new InvalidSpecException(
            InvalidVersionStoreConfig,
            "Invalid cache configuration: spec.versionStore.cache.minFreeHeap must be >= 64Mi");
      }
    }
  }
}
