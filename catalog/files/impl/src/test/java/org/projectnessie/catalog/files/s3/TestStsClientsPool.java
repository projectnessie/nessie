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
package org.projectnessie.catalog.files.s3;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.assertj.core.api.AbstractListAssert;
import org.assertj.core.api.ObjectAssert;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.catalog.files.config.ImmutableS3NamedBucketOptions;
import org.projectnessie.catalog.files.config.S3BucketOptions;
import software.amazon.awssdk.services.sts.StsClient;

@ExtendWith(SoftAssertionsExtension.class)
class TestStsClientsPool {

  @InjectSoftAssertions protected SoftAssertions soft;

  @Test
  void testCache() {
    AtomicInteger counter = new AtomicInteger();
    AtomicReference<StsClient> client = new AtomicReference<>();
    StsClientsPool pool =
        new StsClientsPool(
            2,
            (p) -> {
              counter.incrementAndGet();
              return client.get();
            },
            Optional.empty());

    S3BucketOptions options1 = ImmutableS3NamedBucketOptions.builder().region("R1").build();
    S3BucketOptions options2 = ImmutableS3NamedBucketOptions.builder().region("R2").build();

    StsClient client1 = mock(StsClient.class);
    StsClient client2 = mock(StsClient.class);

    client.set(client1);

    soft.assertThat(pool.stsClientForBucket(options1)).isSameAs(client1);
    soft.assertThat(counter.get()).isEqualTo(1);
    soft.assertThat(pool.stsClientForBucket(options1)).isSameAs(client1);
    soft.assertThat(counter.get()).isEqualTo(1);

    client.set(client2);

    soft.assertThat(pool.stsClientForBucket(options2)).isSameAs(client2);
    soft.assertThat(counter.get()).isEqualTo(2);
    soft.assertThat(pool.stsClientForBucket(options2)).isSameAs(client2);
    soft.assertThat(counter.get()).isEqualTo(2);
  }

  @Test
  void testMetrics() {
    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();

    new StsClientsPool(10, (p) -> null, Optional.of(meterRegistry));

    Function<Meter, AbstractListAssert<?, List<?>, Object, ObjectAssert<Object>>> extractor =
        meter ->
            assertThat(meter)
                .extracting(
                    m -> m.getId().getTag("cache"),
                    m -> m.getId().getName(),
                    m -> m.measure().iterator().next().getValue());

    soft.assertThat(meterRegistry.getMeters())
        .describedAs(meterRegistry.getMetersAsString())
        .anySatisfy(m -> extractor.apply(m).containsExactly("sts-clients", "cache.loads", 0.0d))
        .anySatisfy(m -> extractor.apply(m).containsExactly("sts-clients", "max_entries", 10.0d));
  }
}
