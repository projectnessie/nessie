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
package org.projectnessie.quarkus.providers.storage;

import static java.util.Collections.singletonList;
import static org.projectnessie.quarkus.providers.storage.AddressResolver.LOCAL_ADDRESSES;

import io.vertx.core.Vertx;
import io.vertx.core.dns.DnsClient;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SoftAssertionsExtension.class)
public class TestAddressResolver {
  @InjectSoftAssertions protected SoftAssertions soft;

  protected Vertx vertx;
  protected DnsClient dnsClient;

  @BeforeEach
  void setUp() {
    vertx = Vertx.builder().build();
    dnsClient = vertx.createDnsClient();
  }

  @AfterEach
  void tearDown() throws Exception {
    try {
      dnsClient.close().toCompletionStage().toCompletableFuture().get(1, TimeUnit.MINUTES);
    } finally {
      dnsClient = null;
      try {
        vertx.close().toCompletionStage().toCompletableFuture().get(1, TimeUnit.MINUTES);
      } finally {
        vertx = null;
      }
    }
  }

  @Test
  public void resolveNoName() throws Exception {
    soft.assertThat(
            new AddressResolver(dnsClient)
                .resolveAll(Collections.emptyList())
                .toCompletionStage()
                .toCompletableFuture()
                .get(1, TimeUnit.MINUTES))
        .isEmpty();
  }

  @Test
  @DisabledOnOs(value = OS.MAC, disabledReason = "Resolving 'localhost' doesn't work on macOS")
  public void resolveSingleName() throws Exception {
    soft.assertThat(
            new AddressResolver(dnsClient)
                .resolveAll(singletonList("localhost"))
                .toCompletionStage()
                .toCompletableFuture()
                .get(1, TimeUnit.MINUTES))
        .isNotEmpty()
        .containsAnyOf("0:0:0:0:0:0:0:1", "127.0.0.1");
  }

  @Test
  public void resolveBadName() throws Exception {
    soft.assertThat(
            new AddressResolver(dnsClient)
                .resolveAll(singletonList("wepofkjeopiwkf.wepofkeowpkfpoew.weopfkewopfk.local"))
                .toCompletionStage()
                .toCompletableFuture()
                .get(1, TimeUnit.MINUTES))
        .isEmpty();
  }

  @Test
  public void resolveFilterLocalAddresses() throws Exception {
    soft.assertThat(
            new AddressResolver(dnsClient)
                .resolveAll(singletonList("localhost"))
                .map(s -> s.filter(adr -> !LOCAL_ADDRESSES.contains(adr)).toList())
                .toCompletionStage()
                .toCompletableFuture()
                .get(1, TimeUnit.MINUTES))
        .isEmpty();
  }
}
