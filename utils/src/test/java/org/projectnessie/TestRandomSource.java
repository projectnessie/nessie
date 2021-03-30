/*
 * Copyright (C) 2020 Dremio
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
package org.projectnessie;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;
import static org.projectnessie.RandomSource.current;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

/**
 * Test cases for {@code RandomSource}.
 */
public class TestRandomSource {

  @Test
  public void checkRng() throws IOException {
    assertThat(current().rng(), is(sameInstance(ThreadLocalRandom.current())));
    try (final Closeable c = RandomSource.withSeed(42L)) {
      assertThat(current().rng(), is(not(sameInstance(ThreadLocalRandom.current()))));
    }
    assertThat(current().rng(), is(sameInstance(ThreadLocalRandom.current())));

  }

  @Test
  public void nextBytes() {
    doCheckNextMethod(r -> {
      byte[] b = new byte[42];
      r.nextBytes(b);
      return b;
    });
  }

  @Test
  public void nextInt() {
    doCheckNextMethod(Random::nextInt);
  }

  public void nextIntWithBound() {
    doCheckNextMethod(r -> r.nextInt(42));
  }

  @Test
  public void nextLong() {
    doCheckNextMethod(Random::nextLong);
  }

  @Test
  public void nextBoolean() {
    doCheckNextMethod(Random::nextBoolean);
  }

  @Test
  public void nextFloat() {
    doCheckNextMethod(Random::nextFloat);
  }

  @Test
  public void nextDouble() {
    doCheckNextMethod(Random::nextDouble);
  }

  @Test
  public void nextGaussian() {
    doCheckNextMethod(Random::nextGaussian);
  }

  @Test
  public void intsWithStreamSize() {
    doCheckIntStreamMethod(r -> r.ints(42));
  }

  @Test
  public void ints() {
    doCheckIntStreamMethod(Random::ints);
  }

  @Test
  public void intsWithStreamSizeAndBoundaries() {
    doCheckIntStreamMethod(r -> r.ints(42, 18, 127));
  }

  @Test
  public void intsWithdBoundaries() {
    doCheckIntStreamMethod(r -> r.ints(18, 127));
  }

  private void doCheckIntStreamMethod(Function<Random, IntStream> method) {
    doCheckStreamMethod(method.andThen(s -> s.mapToObj(Integer::valueOf)));
  }

  @Test
  public void longsWithStreamSize() {
    doCheckLongStreamMethod(r -> r.longs(42));
  }

  @Test
  public void longs() {
    doCheckLongStreamMethod(Random::longs);
  }

  @Test
  public void longsWithStreamSizeAndBoundaries() {
    doCheckLongStreamMethod(r -> r.longs(42, 18, 127));
  }

  @Test
  public void longsWithBoundaries() {
    doCheckLongStreamMethod(r -> r.longs(18, 127));
  }

  private void doCheckLongStreamMethod(Function<Random, LongStream> method) {
    doCheckStreamMethod(method.andThen(s -> s.mapToObj(Long::valueOf)));
  }

  @Test
  public void doublesWithStreamSize() {
    doCheckDoubleStreamMethod(r -> r.doubles(42));
  }

  @Test
  public void doubles() {
    doCheckDoubleStreamMethod(Random::doubles);
  }

  @Test
  public void doublesWithStreamSizeAndBoundaries() {
    doCheckDoubleStreamMethod(r -> r.doubles(42, 13.37d, 314.15d));
  }

  @Test
  public void doublesWithAndBoundaries() {
    doCheckDoubleStreamMethod(r -> r.doubles(13.37d, 314.15d));
  }

  private void doCheckDoubleStreamMethod(Function<Random, DoubleStream> method) {
    doCheckStreamMethod(method.andThen(s -> s.mapToObj(Double::valueOf)));
  }

  private <T> void doCheckNextMethod(Function<Random, T> method) {
    long seed = ThreadLocalRandom.current().nextLong();
    Random r = new Random(seed);
    try (final Closeable c = RandomSource.withSeed(seed)) {
      for (int i = 0; i < 100; i++) {
        assertThat(method.apply(RandomSource.current()), is(method.apply(r)));
      }
    }
  }

  private <T> void doCheckStreamMethod(Function<Random, Stream<T>> method) {
    doCheckNextMethod(method.andThen(s -> s.limit(100).collect(Collectors.toList())));
  }
}
