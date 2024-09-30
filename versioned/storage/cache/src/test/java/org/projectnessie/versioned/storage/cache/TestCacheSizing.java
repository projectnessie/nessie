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
package org.projectnessie.versioned.storage.cache;

import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SoftAssertionsExtension.class)
public class TestCacheSizing {
  static final long BYTES_1G = 1024 * 1024 * 1024;
  static final long BYTES_512M = 512 * 1024 * 1024;
  static final long BYTES_4G = 4L * 1024 * 1024 * 1024;
  static final long BYTES_256M = 256 * 1024 * 1024;
  @InjectSoftAssertions protected SoftAssertions soft;

  @Test
  void illegalFractionSettings() {
    soft.assertThatIllegalStateException()
        .isThrownBy(() -> CacheSizing.builder().fractionOfMaxHeapSize(-.1d).build());
    soft.assertThatIllegalStateException()
        .isThrownBy(() -> CacheSizing.builder().fractionOfMaxHeapSize(1.1d).build());
  }

  @Test
  void illegalFixedSettings() {
    soft.assertThatIllegalStateException()
        .isThrownBy(() -> CacheSizing.builder().fixedSizeInMB(-1).build());
  }

  @Test
  void fixedSizeWins() {
    soft.assertThat(
            CacheSizing.builder()
                .fixedSizeInMB(3)
                .fractionOfMaxHeapSize(.5)
                .build()
                .calculateEffectiveSizeInMB(BYTES_512M))
        .isEqualTo(3);
  }

  @Test
  void tinyHeap() {
    // Assuming a 256MB max heap, requesting 70% (358MB), calc yields 64MB (min-size)
    soft.assertThat(
            CacheSizing.builder()
                .fractionOfMaxHeapSize(.7)
                .fractionMinSizeMb(64)
                .build()
                .calculateEffectiveSizeInMB(BYTES_256M))
        .isEqualTo(64);
  }

  @Test
  void tinyHeapNoCache() {
    // Assuming a 256MB max heap, requesting 70% (179MB), calc yields fractionMinSizeMb, i.e. zero
    soft.assertThat(
            CacheSizing.builder()
                .fractionOfMaxHeapSize(.7)
                .fractionMinSizeMb(0)
                .build()
                .calculateEffectiveSizeInMB(BYTES_256M))
        .isEqualTo(0);
  }

  @Test
  void defaultSettings4G() {
    // Assuming a 4G max heap, requesting 70% (358MB), sizing must yield 2867MB.
    soft.assertThat(CacheSizing.builder().build().calculateEffectiveSizeInMB(BYTES_4G))
        .isEqualTo(2457);
  }

  @Test
  void defaultSettings1G() {
    soft.assertThat(CacheSizing.builder().build().calculateEffectiveSizeInMB(BYTES_1G))
        // 70 % of 1024 MB
        .isEqualTo(614);
  }

  @Test
  void defaultSettingsTiny() {
    soft.assertThat(CacheSizing.builder().build().calculateEffectiveSizeInMB(BYTES_256M))
        // 70 % of 1024 MB
        .isEqualTo(64);
  }

  @Test
  void turnOff() {
    soft.assertThat(
            CacheSizing.builder().fixedSizeInMB(0).build().calculateEffectiveSizeInMB(BYTES_1G))
        // 70 % of 1024 MB
        .isEqualTo(0);
  }

  @Test
  void keepsHeapFree() {
    // Assuming a 512MB max heap, requesting 70% (358MB), exceeds "min free" of 256MB, sizing must
    // yield 256MB.
    soft.assertThat(
            CacheSizing.builder()
                .fractionOfMaxHeapSize(.7)
                .build()
                .calculateEffectiveSizeInMB(BYTES_512M))
        .isEqualTo(256);
  }
}
