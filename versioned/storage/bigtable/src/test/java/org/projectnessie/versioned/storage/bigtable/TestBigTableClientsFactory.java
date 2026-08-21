/*
 * Copyright (C) 2026 Dremio
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
package org.projectnessie.versioned.storage.bigtable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;
import static org.projectnessie.versioned.storage.bigtable.BigTableClientsFactory.channelPoolSettings;

import com.google.api.gax.grpc.ChannelPoolSettings;
import com.google.cloud.bigtable.data.v2.stub.EnhancedBigtableStubSettings;
import java.util.OptionalInt;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TestBigTableClientsFactory {

  @Test
  void unconfiguredChannelPoolSettingsInheritsDefaults() {
    ChannelPoolSettings inherited = defaultChannelPoolSettings();
    ChannelPoolSettings actual =
        channelPoolSettings(inherited, Mockito.mock(BigTableClientsConfig.class));
    assertThat(actual).isEqualTo(inherited);

    // With nothing configured, the pool must remain dynamically sized.
    // If ChannelPoolSettings.isStaticSize() returns true, that stops gax from ever resizing the
    // pool and pins it to a single channel. isStaticSize() is not visible here, so its two
    // conditions are asserted directly:

    assertThat(actual.getMinChannelCount())
        .describedAs("min-channel-count == max-channel-count makes the pool statically sized")
        .isNotEqualTo(actual.getMaxChannelCount());
    assertThat(
            actual.getMinRpcsPerChannel() == 0
                && actual.getMaxRpcsPerChannel() == Integer.MAX_VALUE)
        .describedAs(
            "min-rpcs-per-channel=0 together with max-rpcs-per-channel=Integer.MAX_VALUE makes the"
                + " pool statically sized")
        .isFalse();
    assertThat(actual.getInitialChannelCount()).isGreaterThan(1);
  }

  @Test
  void fullyConfiguredChannelPoolSettingsOverrideInheritedDefaults() {
    BigTableClientsConfig config = Mockito.mock(BigTableClientsConfig.class);
    when(config.minChannelCount()).thenReturn(OptionalInt.of(4));
    when(config.maxChannelCount()).thenReturn(OptionalInt.of(16));
    when(config.initialChannelCount()).thenReturn(OptionalInt.of(8));
    when(config.minRpcsPerChannel()).thenReturn(OptionalInt.of(2));
    when(config.maxRpcsPerChannel()).thenReturn(OptionalInt.of(50));

    ChannelPoolSettings settings = channelPoolSettings(defaultChannelPoolSettings(), config);

    assertThat(settings.getMinChannelCount()).isEqualTo(4);
    assertThat(settings.getMaxChannelCount()).isEqualTo(16);
    assertThat(settings.getInitialChannelCount()).isEqualTo(8);
    assertThat(settings.getMinRpcsPerChannel()).isEqualTo(2);
    assertThat(settings.getMaxRpcsPerChannel()).isEqualTo(50);
  }

  @Test
  void partiallyConfiguredChannelPoolSettingsLeavesTheRestInherited() {
    ChannelPoolSettings inherited = defaultChannelPoolSettings();

    BigTableClientsConfig config = Mockito.mock(BigTableClientsConfig.class);
    when(config.maxChannelCount()).thenReturn(OptionalInt.of(42));

    ChannelPoolSettings settings = channelPoolSettings(inherited, config);

    assertThat(settings.getMaxChannelCount()).isEqualTo(42);
    assertThat(settings.getMinChannelCount()).isEqualTo(inherited.getMinChannelCount());
    assertThat(settings.getInitialChannelCount()).isEqualTo(inherited.getInitialChannelCount());
    assertThat(settings.getMinRpcsPerChannel()).isEqualTo(inherited.getMinRpcsPerChannel());
    assertThat(settings.getMaxRpcsPerChannel()).isEqualTo(inherited.getMaxRpcsPerChannel());
    assertThat(settings.isPreemptiveRefreshEnabled())
        .isEqualTo(inherited.isPreemptiveRefreshEnabled());
  }

  @Test
  void configuredMaxChannelCountBelowInheritedInitialChannelCount() {
    BigTableClientsConfig config = Mockito.mock(BigTableClientsConfig.class);
    when(config.maxChannelCount()).thenReturn(OptionalInt.of(1));

    ChannelPoolSettings settings = channelPoolSettings(defaultChannelPoolSettings(), config);

    assertThat(settings.getMaxChannelCount()).isEqualTo(1);
    assertThat(settings.getInitialChannelCount()).isEqualTo(1);
    assertThat(settings.getMinChannelCount()).isEqualTo(1);
  }

  @Test
  void configuredMinChannelCountAboveInheritedInitialChannelCount() {
    ChannelPoolSettings inherited = defaultChannelPoolSettings();
    int min = inherited.getInitialChannelCount() + 5;

    BigTableClientsConfig config = Mockito.mock(BigTableClientsConfig.class);
    when(config.minChannelCount()).thenReturn(OptionalInt.of(min));
    // Required because min-channel-count must not exceed max-rpcs-per-channel.
    when(config.maxRpcsPerChannel()).thenReturn(OptionalInt.of(min));

    ChannelPoolSettings settings = channelPoolSettings(inherited, config);

    assertThat(settings.getMinChannelCount()).isEqualTo(min);
    assertThat(settings.getInitialChannelCount()).isEqualTo(min);
    assertThat(settings.getMaxChannelCount()).isEqualTo(inherited.getMaxChannelCount());
  }

  @Test
  void configuredMinChannelCountAboveInheritedMaxChannelCount() {
    ChannelPoolSettings inherited = defaultChannelPoolSettings();
    int min = inherited.getMaxChannelCount() + 5;

    BigTableClientsConfig config = Mockito.mock(BigTableClientsConfig.class);
    when(config.minChannelCount()).thenReturn(OptionalInt.of(min));
    when(config.maxRpcsPerChannel()).thenReturn(OptionalInt.of(min));

    ChannelPoolSettings settings = channelPoolSettings(inherited, config);

    assertThat(settings.getMinChannelCount()).isEqualTo(min);
    assertThat(settings.getMaxChannelCount()).isEqualTo(min);
    assertThat(settings.getInitialChannelCount()).isEqualTo(min);
  }

  @Test
  void configuredMinRpcsPerChannelAboveInheritedMaxRpcsPerChannel() {
    ChannelPoolSettings inherited = defaultChannelPoolSettings();
    int minRpcs = inherited.getMaxRpcsPerChannel() + 10;

    BigTableClientsConfig config = Mockito.mock(BigTableClientsConfig.class);
    when(config.minRpcsPerChannel()).thenReturn(OptionalInt.of(minRpcs));

    ChannelPoolSettings settings = channelPoolSettings(inherited, config);

    assertThat(settings.getMinRpcsPerChannel()).isEqualTo(minRpcs);
    assertThat(settings.getMaxRpcsPerChannel()).isEqualTo(minRpcs);
  }

  /**
   * A configured maximum below the inherited minimum RPCs per channel lowers the latter. The
   * inherited settings are synthetic here: the Bigtable client library's own minimum is 1, and the
   * only value below that, 0, is rejected by an unrelated {@code ChannelPoolSettings} precondition
   * requiring {@code min-channel-count <= max-rpcs-per-channel}.
   */
  @Test
  void configuredMaxRpcsPerChannelBelowInheritedMinRpcsPerChannel() {
    ChannelPoolSettings inherited =
        defaultChannelPoolSettings().toBuilder().setMinRpcsPerChannel(20).build();

    BigTableClientsConfig config = Mockito.mock(BigTableClientsConfig.class);
    when(config.maxRpcsPerChannel()).thenReturn(OptionalInt.of(5));

    ChannelPoolSettings settings = channelPoolSettings(inherited, config);

    assertThat(settings.getMaxRpcsPerChannel()).isEqualTo(5);
    assertThat(settings.getMinRpcsPerChannel()).isEqualTo(5);
  }

  @Test
  void conflictingExplicitRpcsPerChannelAreRejected() {
    BigTableClientsConfig config = Mockito.mock(BigTableClientsConfig.class);
    when(config.minRpcsPerChannel()).thenReturn(OptionalInt.of(10));
    when(config.maxRpcsPerChannel()).thenReturn(OptionalInt.of(5));

    ChannelPoolSettings defaults = defaultChannelPoolSettings();
    assertThatThrownBy(() -> channelPoolSettings(defaults, config))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("rpcsPerChannel range is invalid");
  }

  @Test
  void conflictingExplicitChannelCountsAreRejected() {
    BigTableClientsConfig config = Mockito.mock(BigTableClientsConfig.class);
    when(config.initialChannelCount()).thenReturn(OptionalInt.of(10));
    when(config.maxChannelCount()).thenReturn(OptionalInt.of(1));

    ChannelPoolSettings defaults = defaultChannelPoolSettings();
    assertThatThrownBy(() -> channelPoolSettings(defaults, config))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("initial channel count");
  }

  @Test
  void staticallySizedChannelPoolSettingsArePreserved() {
    ChannelPoolSettings inherited = ChannelPoolSettings.staticallySized(1);
    ChannelPoolSettings actual =
        channelPoolSettings(inherited, Mockito.mock(BigTableClientsConfig.class));
    assertThat(actual).isEqualTo(inherited);
  }

  private static ChannelPoolSettings defaultChannelPoolSettings() {
    return EnhancedBigtableStubSettings.defaultGrpcTransportProviderBuilder()
        .build()
        .getChannelPoolSettings();
  }
}
