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
package org.projectnessie.tools.contentgenerator.keygen;

import java.util.List;
import java.util.Random;
import javax.annotation.Nonnull;
import org.immutables.value.Value;

public class KeyGenerator {

  @Value.Immutable
  public interface Params {
    @Value.Default
    default long seed() {
      return new Random().nextLong();
    }

    String pattern();
  }

  @FunctionalInterface
  public interface Func {
    void apply(Random random, StringBuilder target);
  }

  public static KeyGenerator newKeyGenerator(@Nonnull String pattern) {
    return newKeyGenerator(ImmutableParams.builder().pattern(pattern).build());
  }

  public static KeyGenerator newKeyGenerator(long seed, @Nonnull String pattern) {
    return newKeyGenerator(ImmutableParams.builder().seed(seed).pattern(pattern).build());
  }

  public static KeyGenerator newKeyGenerator(@Nonnull Params params) {
    PatternParser parser = new PatternParser(params.pattern());
    List<Func> generators = parser.parse();
    return new KeyGenerator(params, generators);
  }

  private final Params params;
  private final Random random;
  private final List<Func> generators;

  private KeyGenerator(@Nonnull Params params, @Nonnull List<Func> generators) {
    this.params = params;
    this.random = new Random(params.seed());
    this.generators = generators;
  }

  public String generate() {
    StringBuilder sb = new StringBuilder();
    generate(sb);
    return sb.toString();
  }

  public void generate(StringBuilder target) {
    generators.forEach(f -> f.apply(random, target));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof KeyGenerator)) {
      return false;
    }
    KeyGenerator that = (KeyGenerator) o;
    return params.equals(that.params);
  }

  @Override
  public int hashCode() {
    return params.hashCode();
  }

  @Override
  public String toString() {
    return "KeyGenerator{" + "params=" + params + '}';
  }
}
