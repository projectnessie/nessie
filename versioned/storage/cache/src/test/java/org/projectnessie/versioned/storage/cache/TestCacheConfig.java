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
package org.projectnessie.versioned.storage.cache;

import static org.projectnessie.versioned.storage.cache.CacheConfig.INVALID_REFERENCE_NEGATIVE_TTL;
import static org.projectnessie.versioned.storage.cache.CacheConfig.INVALID_REFERENCE_TTL;

import java.time.Duration;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SoftAssertionsExtension.class)
public class TestCacheConfig {
  @InjectSoftAssertions protected SoftAssertions soft;

  @Test
  public void allDefaults() {
    soft.assertThatCode(() -> defaultBuilder().build()).doesNotThrowAnyException();
  }

  @Test
  public void referenceCaching() {
    soft.assertThatCode(() -> defaultBuilder().referenceTtl(Duration.ofMinutes(1)).build())
        .doesNotThrowAnyException();
    soft.assertThatCode(
            () ->
                defaultBuilder()
                    .referenceTtl(Duration.ofMinutes(1))
                    .referenceNegativeTtl(Duration.ofMinutes(1))
                    .build())
        .doesNotThrowAnyException();
    soft.assertThatIllegalStateException()
        .isThrownBy(() -> defaultBuilder().referenceTtl(Duration.ofMinutes(0)).build())
        .withMessage(INVALID_REFERENCE_TTL);
    soft.assertThatIllegalStateException()
        .isThrownBy(() -> defaultBuilder().referenceTtl(Duration.ofMinutes(-1)).build())
        .withMessage(INVALID_REFERENCE_TTL);
    soft.assertThatIllegalStateException()
        .isThrownBy(() -> defaultBuilder().referenceNegativeTtl(Duration.ofMinutes(1)).build())
        .withMessage(INVALID_REFERENCE_NEGATIVE_TTL);
    soft.assertThatIllegalStateException()
        .isThrownBy(
            () ->
                defaultBuilder()
                    .referenceTtl(Duration.ofMinutes(1))
                    .referenceNegativeTtl(Duration.ofMinutes(-1))
                    .build())
        .withMessage(INVALID_REFERENCE_NEGATIVE_TTL);
    soft.assertThatIllegalStateException()
        .isThrownBy(
            () ->
                defaultBuilder()
                    .referenceTtl(Duration.ofMinutes(1))
                    .referenceNegativeTtl(Duration.ofMinutes(0))
                    .build())
        .withMessage(INVALID_REFERENCE_NEGATIVE_TTL);
  }

  private static CacheConfig.Builder defaultBuilder() {
    return CacheConfig.builder().capacityMb(1).cacheCapacityOvershoot(0.1d);
  }
}
