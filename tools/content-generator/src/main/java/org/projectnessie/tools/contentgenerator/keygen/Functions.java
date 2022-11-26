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

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.UUID.randomUUID;

import java.util.Deque;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import org.projectnessie.tools.contentgenerator.keygen.KeyGenerator.Func;

public final class Functions {

  private Functions() {}

  public static FuncGenerator resolveFunction(String function) {
    FuncGenerator generator = FUNCTIONS.get(function.toLowerCase(Locale.ROOT));
    if (generator == null) {
      throw new IllegalArgumentException("Unknown function '" + function + "'");
    }
    return generator;
  }

  static Map<String, FuncGenerator> FUNCTIONS = new HashMap<>();

  static {
    FUNCTIONS.put("uuid", params -> Optional.of(new UuidFunc()));
    FUNCTIONS.put(
        "int",
        params -> {
          long bound = FuncGenerator.longParam(params);
          return Optional.of(new IntFunc(bound));
        });
    FUNCTIONS.put(
        "string",
        params -> {
          int len = FuncGenerator.intParam(params);
          return Optional.of(new StringFunc(len));
        });
    FUNCTIONS.put(
        "seq",
        params -> {
          int offset = FuncGenerator.intParam(params);
          return Optional.of(new SeqFunc(offset));
        });
    FUNCTIONS.put(
        "prob",
        params -> {
          double prob = FuncGenerator.doubleParam(params);
          Func func = FuncGenerator.funcParam(params);
          return Optional.of(new ProbFunc(prob, func));
        });
    FUNCTIONS.put(
        "every",
        params -> {
          int seq = FuncGenerator.intParam(params);
          Func func = FuncGenerator.funcParam(params);
          return Optional.of(new EveryFunc(seq, func));
        });
  }

  @FunctionalInterface
  interface FuncGenerator {
    Optional<Func> generate(Deque<String> params);

    static double doubleParam(Deque<String> params) {
      return Double.parseDouble(params.removeFirst());
    }

    static int intParam(Deque<String> params) {
      return Integer.parseInt(params.removeFirst());
    }

    static long longParam(Deque<String> params) {
      return Long.parseLong(params.removeFirst());
    }

    static Func funcParam(Deque<String> params) {
      String name = params.removeFirst();
      return resolveFunction(name)
          .generate(params)
          .orElseThrow(
              () ->
                  new IllegalArgumentException(
                      "Nested function pattern did not resolve to a function"));
    }
  }

  static final class UuidFunc implements Func {
    UuidFunc() {}

    @Override
    public void apply(Random random, StringBuilder target) {
      target.append(randomUUID());
    }
  }

  static final class IntFunc implements Func {
    private final long bound;

    IntFunc(long bound) {
      checkArgument(bound > 0, "Bound for random must be positive");
      this.bound = bound;
    }

    @Override
    public void apply(Random random, StringBuilder target) {
      long v = (random.nextLong() & Long.MAX_VALUE) % bound;
      target.append(v);
    }
  }

  static final class StringFunc implements Func {
    private final int len;

    private static final char[] CHARS =
        ("ABCDEFGHIJKLMNOPQRSTUVWXYZ" + "abcdefghijklmnopqrstuvwxyz" + "0123456789" + "b._-")
            .toCharArray();

    StringFunc(int len) {
      this.len = len;
    }

    @Override
    public void apply(Random random, StringBuilder target) {
      for (int i = 0; i < len; i++) {
        char c = CHARS[random.nextInt(CHARS.length)];
        target.append(c);
      }
    }
  }

  static final class SeqFunc implements Func {
    private int num;

    SeqFunc(int offset) {
      this.num = offset;
    }

    @Override
    public void apply(Random random, StringBuilder target) {
      target.append(num);
      num++;
    }
  }

  static final class EveryFunc implements Func {
    private final int seq;
    private final Func delegate;
    private int num;
    private String current;

    EveryFunc(int seq, Func delegate) {
      this.seq = this.num = seq;
      this.delegate = delegate;
    }

    @Override
    public void apply(Random random, StringBuilder target) {
      if (num >= seq) {
        current = null;
      }
      if (current == null) {
        StringBuilder sb = new StringBuilder();
        delegate.apply(random, sb);
        current = sb.toString();
        num = 1;
      } else {
        num++;
      }
      target.append(current);
    }
  }

  static final class ProbFunc implements Func {
    private final double prob;
    private final Func delegate;
    private String current;

    ProbFunc(double prob, Func delegate) {
      this.prob = prob;
      this.delegate = delegate;
    }

    @Override
    public void apply(Random random, StringBuilder target) {
      double v = random.nextDouble();
      if (v < prob) {
        current = null;
      }
      if (current == null) {
        StringBuilder sb = new StringBuilder();
        delegate.apply(random, sb);
        current = sb.toString();
      }
      target.append(current);
    }
  }
}
