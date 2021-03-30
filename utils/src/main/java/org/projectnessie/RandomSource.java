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

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import com.google.errorprone.annotations.CheckReturnValue;

/**
 * A random generator similar to {@code ThreadLocalRandom}
 * but which allows to use a different seed in test contexts.
 */
@SuppressWarnings("serial")
public class RandomSource extends Random {
  private static final ThreadLocal<Random> RNG = ThreadLocal.withInitial(ThreadLocalRandom::current);

  private static final RandomSource INSTANCE = new RandomSource();

  private final boolean isInitialized;

  private RandomSource() {
    this.isInitialized = true;
  }

  /**
   * Get the current random generator.
   *
   * <p>The instance validity is only guaranteed for the current thread.
   * As a consequence it is not recommend to store the result of this method.
   * @return the current random generator
   */
  public static RandomSource current() {
    return INSTANCE;
  }

  /**
   * Changes the current random generator with a new one.
   *
   * <p>The change is only valid in the current thread, until the context is
   * restored by closing the returned object.
   *
   * @param seed the seed to initialize the new random generator with.
   * @return a {@code Closeable} instance to restore the previous conext
   */
  @CheckReturnValue
  public static Closeable withSeed(long seed) {
    final Random previousRng = RNG.get();
    RNG.set(new Random(seed));
    return () -> RNG.set(previousRng);
  }

  @Override
  public synchronized void setSeed(long seed) {
    if (!isInitialized) {
      return;
    }
    throw new UnsupportedOperationException();
  }

  @Override
  public void nextBytes(byte[] bytes) {
    super.nextBytes(bytes);
  }

  @Override
  public int nextInt() {
    return rng().nextInt();
  }

  @Override
  public int nextInt(int bound) {
    return rng().nextInt(bound);
  }

  @Override
  public long nextLong() {
    return rng().nextLong();
  }

  @Override
  public boolean nextBoolean() {
    return rng().nextBoolean();
  }

  @Override
  public float nextFloat() {
    return rng().nextFloat();
  }

  @Override
  public double nextDouble() {
    return rng().nextDouble();
  }

  @Override
  public synchronized double nextGaussian() {
    return rng().nextGaussian();
  }

  @Override
  public IntStream ints(long streamSize) {
    return rng().ints(streamSize);
  }

  @Override
  public IntStream ints() {
    return rng().ints();
  }

  @Override
  public IntStream ints(long streamSize, int randomNumberOrigin, int randomNumberBound) {
    return rng().ints(streamSize, randomNumberOrigin, randomNumberBound);
  }

  @Override
  public IntStream ints(int randomNumberOrigin, int randomNumberBound) {
    return rng().ints(randomNumberOrigin, randomNumberBound);
  }

  @Override
  public LongStream longs(long streamSize) {
    return rng().longs(streamSize);
  }

  @Override
  public LongStream longs() {
    return rng().longs();
  }

  @Override
  public LongStream longs(long streamSize, long randomNumberOrigin,
      long randomNumberBound) {
    return rng().longs(streamSize, randomNumberOrigin, randomNumberBound);
  }

  @Override
  public LongStream longs(long randomNumberOrigin, long randomNumberBound) {
    return rng().longs(randomNumberOrigin, randomNumberBound);
  }

  @Override
  public DoubleStream doubles(long streamSize) {
    return rng().doubles(streamSize);
  }

  @Override
  public DoubleStream doubles() {
    return rng().doubles();
  }

  @Override
  public DoubleStream doubles(long streamSize, double randomNumberOrigin,
      double randomNumberBound) {
    return rng().doubles(streamSize, randomNumberOrigin, randomNumberBound);
  }

  @Override
  public DoubleStream doubles(double randomNumberOrigin, double randomNumberBound) {
    return rng().doubles(randomNumberOrigin, randomNumberBound);
  }

  // Visible for testing
  final Random rng() {
    return RNG.get();
  }
}
