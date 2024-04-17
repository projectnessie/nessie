/*
 * Copyright (C) 2022 Dremio
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
package org.projectnessie.versioned.storage.common.config;

import static java.util.Collections.singletonMap;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.projectnessie.versioned.storage.common.config.StoreConfig.CONFIG_ASSUMED_WALL_CLOCK_DRIFT_MICROS;
import static org.projectnessie.versioned.storage.common.config.StoreConfig.CONFIG_COMMIT_RETRIES;
import static org.projectnessie.versioned.storage.common.config.StoreConfig.CONFIG_COMMIT_TIMEOUT_MILLIS;
import static org.projectnessie.versioned.storage.common.config.StoreConfig.CONFIG_MAX_INCREMENTAL_INDEX_SIZE;
import static org.projectnessie.versioned.storage.common.config.StoreConfig.CONFIG_MAX_REFERENCE_STRIPES_PER_COMMIT;
import static org.projectnessie.versioned.storage.common.config.StoreConfig.CONFIG_MAX_SERIALIZED_INDEX_SIZE;
import static org.projectnessie.versioned.storage.common.config.StoreConfig.CONFIG_NAMESPACE_VALIDATION;
import static org.projectnessie.versioned.storage.common.config.StoreConfig.CONFIG_PARENTS_PER_COMMIT;
import static org.projectnessie.versioned.storage.common.config.StoreConfig.CONFIG_REPOSITORY_ID;
import static org.projectnessie.versioned.storage.common.config.StoreConfig.CONFIG_RETRY_INITIAL_SLEEP_MILLIS_LOWER;
import static org.projectnessie.versioned.storage.common.config.StoreConfig.CONFIG_RETRY_INITIAL_SLEEP_MILLIS_UPPER;
import static org.projectnessie.versioned.storage.common.config.StoreConfig.CONFIG_RETRY_MAX_SLEEP_MILLIS;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.versioned.storage.common.config.StoreConfig.Adjustable;

@ExtendWith(SoftAssertionsExtension.class)
public class TestStoreConfig {

  public static final int REFERENCE_TIME = 1000000;
  @InjectSoftAssertions SoftAssertions soft;

  static Stream<Arguments> adjustable() {
    return Stream.of(
        // config properties
        arguments(
            CONFIG_REPOSITORY_ID,
            "foobar",
            (Function<Adjustable, StoreConfig>) e -> e.withRepositoryId("foobar"),
            (Predicate<StoreConfig>) c -> c.repositoryId().equals("foobar")),
        arguments(
            CONFIG_COMMIT_RETRIES,
            "22",
            (Function<Adjustable, StoreConfig>) e -> e.withCommitRetries(22),
            (Predicate<StoreConfig>) c -> c.commitRetries() == 22),
        arguments(
            CONFIG_COMMIT_TIMEOUT_MILLIS,
            "1234",
            (Function<Adjustable, StoreConfig>) e -> e.withCommitTimeoutMillis(1234),
            (Predicate<StoreConfig>) c -> c.commitTimeoutMillis() == 1234),
        arguments(
            CONFIG_RETRY_INITIAL_SLEEP_MILLIS_LOWER,
            "4321",
            (Function<Adjustable, StoreConfig>) e -> e.withRetryInitialSleepMillisLower(4321),
            (Predicate<StoreConfig>) c -> c.retryInitialSleepMillisLower() == 4321),
        arguments(
            CONFIG_RETRY_INITIAL_SLEEP_MILLIS_UPPER,
            "54321",
            (Function<Adjustable, StoreConfig>) e -> e.withRetryInitialSleepMillisUpper(54321),
            (Predicate<StoreConfig>) c -> c.retryInitialSleepMillisUpper() == 54321),
        arguments(
            CONFIG_RETRY_MAX_SLEEP_MILLIS,
            "99999",
            (Function<Adjustable, StoreConfig>) e -> e.withRetryMaxSleepMillis(99999),
            (Predicate<StoreConfig>) c -> c.retryMaxSleepMillis() == 99999),
        arguments(
            CONFIG_PARENTS_PER_COMMIT,
            "123",
            (Function<Adjustable, StoreConfig>) e -> e.withParentsPerCommit(123),
            (Predicate<StoreConfig>) c -> c.parentsPerCommit() == 123),
        arguments(
            CONFIG_MAX_INCREMENTAL_INDEX_SIZE,
            "123444",
            (Function<Adjustable, StoreConfig>) e -> e.withMaxIncrementalIndexSize(123444),
            (Predicate<StoreConfig>) c -> c.maxIncrementalIndexSize() == 123444),
        arguments(
            CONFIG_MAX_SERIALIZED_INDEX_SIZE,
            "123456",
            (Function<Adjustable, StoreConfig>) e -> e.withMaxSerializedIndexSize(123456),
            (Predicate<StoreConfig>) c -> c.maxSerializedIndexSize() == 123456),
        arguments(
            CONFIG_MAX_REFERENCE_STRIPES_PER_COMMIT,
            "123654",
            (Function<Adjustable, StoreConfig>) e -> e.withMaxReferenceStripesPerCommit(123654),
            (Predicate<StoreConfig>) c -> c.maxReferenceStripesPerCommit() == 123654),
        arguments(
            CONFIG_ASSUMED_WALL_CLOCK_DRIFT_MICROS,
            "1234567",
            (Function<Adjustable, StoreConfig>) e -> e.withAssumedWallClockDriftMicros(1234567),
            (Predicate<StoreConfig>) c -> c.assumedWallClockDriftMicros() == 1234567),
        arguments(
            CONFIG_NAMESPACE_VALIDATION,
            "false",
            (Function<Adjustable, StoreConfig>) e -> e.withValidateNamespaces(false),
            (Predicate<StoreConfig>)
                c -> {
                  @SuppressWarnings("removal")
                  boolean validateNamespaces = c.validateNamespaces();
                  return !validateNamespaces;
                }),
        // default methods (current time in micros + hasher)
        arguments(
            "x",
            "y",
            (Function<Adjustable, StoreConfig>) e -> e,
            (Predicate<StoreConfig>)
                c -> c.currentTimeMicros() == MILLISECONDS.toMicros(REFERENCE_TIME)));
  }

  @ParameterizedTest
  @MethodSource("adjustable")
  public void adjustable(
      String key,
      String value,
      Function<Adjustable, StoreConfig> expected,
      Predicate<StoreConfig> checker) {
    Clock clock = Clock.fixed(Instant.ofEpochMilli(REFERENCE_TIME), ZoneId.of("GMT"));

    Supplier<Adjustable> template = () -> Adjustable.empty().withClock(clock);

    StoreConfig e = expected.apply(template.get());

    soft.assertThat(template.get().fromFunction(singletonMap(key, value)::get))
        .isEqualTo(e)
        .matches(checker);
  }
}
