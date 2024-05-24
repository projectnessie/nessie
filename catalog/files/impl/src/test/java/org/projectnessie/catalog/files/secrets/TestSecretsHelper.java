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
package org.projectnessie.catalog.files.secrets;

import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.projectnessie.catalog.files.secrets.SecretsHelper.Specializeable.specializable;
import static org.projectnessie.catalog.files.secrets.SecretsProvider.mapSecretsProvider;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.nessie.immutables.NessieImmutable;

@ExtendWith(SoftAssertionsExtension.class)
public class TestSecretsHelper {
  @InjectSoftAssertions protected SoftAssertions soft;

  @ParameterizedTest
  @MethodSource
  public void secretsHelper(Opts base, Opts spec, Opts expected, Map<String, String> secrets) {
    Opts.Builder builder = Opts.builder();
    List<SecretsHelper.Specializeable<Opts, Opts.Builder>> specializables =
        List.of(
            specializable("foo", Opts::foo, Opts.Builder::foo),
            specializable("bar", Opts::bar, Opts.Builder::bar),
            specializable("baz", Opts::baz, Opts.Builder::baz));

    SecretsHelper.resolveSecrets(
        mapSecretsProvider(secrets), "base", builder, base, "spec", spec, specializables);

    soft.assertThat(builder.build()).isEqualTo(expected);
  }

  public static Stream<Arguments> secretsHelper() {
    Opts fooBarBaz = Opts.builder().foo("foo").bar("bar").baz("baz").build();
    Opts nopeNopeNope = Opts.builder().foo("nope").bar("nope").baz("nope").build();
    return Stream.of(
        arguments(Opts.EMPTY, null, Opts.EMPTY, Map.of()),
        arguments(Opts.EMPTY, Opts.EMPTY, Opts.EMPTY, Map.of()),
        //
        arguments(
            Opts.EMPTY,
            Opts.EMPTY,
            fooBarBaz,
            Map.of(
                "base.spec.foo", "foo",
                "base.spec.bar", "bar",
                "base.spec.baz", "baz")),
        arguments(
            Opts.EMPTY,
            null,
            fooBarBaz,
            Map.of(
                "base.spec.foo", "foo",
                "base.spec.bar", "bar",
                "base.spec.baz", "baz")),
        //
        arguments(
            Opts.EMPTY,
            Opts.EMPTY,
            fooBarBaz,
            Map.of(
                "base.foo", "foo",
                "base.bar", "bar",
                "base.baz", "baz")),
        //
        arguments(
            Opts.EMPTY,
            Opts.EMPTY,
            fooBarBaz,
            Map.of(
                "base.spec.foo", "foo",
                "base.spec.bar", "bar",
                "base.spec.baz", "baz",
                "base.foo", "no",
                "base.bar", "no",
                "base.baz", "no")),
        arguments(
            Opts.EMPTY,
            null,
            fooBarBaz,
            Map.of(
                "base.spec.foo", "foo",
                "base.spec.bar", "bar",
                "base.spec.baz", "baz",
                "base.foo", "no",
                "base.bar", "no",
                "base.baz", "no")),
        //
        arguments(
            Opts.EMPTY,
            Opts.EMPTY,
            fooBarBaz,
            Map.of(
                "base.spec.foo", "foo",
                "base.spec.baz", "baz",
                "base.foo", "no",
                "base.bar", "bar",
                "base.baz", "no")),
        arguments(
            Opts.EMPTY,
            null,
            fooBarBaz,
            Map.of(
                "base.spec.foo", "foo",
                "base.spec.baz", "baz",
                "base.foo", "no",
                "base.bar", "bar",
                "base.baz", "no")),
        //
        arguments(
            Opts.EMPTY,
            Opts.builder().bar("bar").build(),
            fooBarBaz,
            Map.of(
                "base.spec.foo", "foo",
                "base.spec.bar", "no-spec",
                "base.spec.baz", "baz",
                "base.foo", "no",
                "base.bar", "no-base",
                "base.baz", "no")),
        //
        arguments(
            Opts.builder().bar("bar").build(),
            Opts.EMPTY,
            fooBarBaz,
            Map.of(
                "base.spec.foo", "foo",
                "base.spec.baz", "baz",
                "base.foo", "no",
                "base.bar", "no-base",
                "base.baz", "no")),
        arguments(
            Opts.builder().bar("bar").build(),
            null,
            fooBarBaz,
            Map.of(
                "base.spec.foo", "foo",
                "base.spec.baz", "baz",
                "base.foo", "no",
                "base.bar", "no-base",
                "base.baz", "no")),
        //
        arguments(
            fooBarBaz,
            Opts.EMPTY,
            fooBarBaz,
            Map.of(
                "base.spec.foo", "foo",
                "base.spec.baz", "baz",
                "base.foo", "no",
                "base.bar", "no-base",
                "base.baz", "no")),
        arguments(
            nopeNopeNope,
            null,
            fooBarBaz,
            Map.of(
                "base.spec.foo", "foo",
                "base.spec.bar", "bar",
                "base.spec.baz", "baz",
                "base.foo", "no",
                "base.bar", "no",
                "base.baz", "no")),
        //
        arguments(fooBarBaz, Opts.EMPTY, fooBarBaz, Map.of()),
        arguments(fooBarBaz, null, fooBarBaz, Map.of()),
        arguments(nopeNopeNope, fooBarBaz, fooBarBaz, Map.of())
        //
        );
  }

  @NessieImmutable
  public interface Opts {
    Opts EMPTY = Opts.builder().build();

    Optional<String> foo();

    Optional<String> bar();

    Optional<String> baz();

    static Builder builder() {
      return ImmutableOpts.builder();
    }

    interface Builder {
      Builder foo(String foo);

      Builder bar(String bar);

      Builder baz(String baz);

      Opts build();
    }
  }
}
