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

import static io.vertx.core.Future.succeededFuture;
import static java.net.NetworkInterface.networkInterfaces;
import static java.util.stream.Collectors.toUnmodifiableSet;

import com.google.common.annotations.VisibleForTesting;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.dns.DnsClient;
import java.net.InetAddress;
import java.net.InterfaceAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@VisibleForTesting
final class AddressResolver {
  private static final Logger LOGGER = LoggerFactory.getLogger(AddressResolver.class);

  private final DnsClient dnsClient;

  static final Set<String> LOCAL_ADDRESSES;

  static {
    try {
      LOCAL_ADDRESSES =
          networkInterfaces()
              .flatMap(
                  ni ->
                      ni.getInterfaceAddresses().stream()
                          // Need to do this InetAddress->byte[]->InetAddress dance to get rid of
                          // host-address suffixes as in `0:0:0:0:0:0:0:1%lo'
                          .map(InterfaceAddress::getAddress)
                          .map(InetAddress::getAddress)
                          .map(
                              a -> {
                                try {
                                  return InetAddress.getByAddress(a);
                                } catch (UnknownHostException e) {
                                  // Should never happen when calling getByAddress() with an IPv4 or
                                  // IPv6 address
                                  throw new RuntimeException(e);
                                }
                              })
                          .map(InetAddress::getHostAddress))
              .collect(toUnmodifiableSet());
    } catch (SocketException e) {
      throw new RuntimeException(e);
    }
  }

  AddressResolver(DnsClient dnsClient) {
    this.dnsClient = dnsClient;
  }

  Future<Stream<String>> resolve(String name) {
    if (name.startsWith("=")) {
      return Future.succeededFuture(Stream.of(name.substring(1)));
    }
    return dnsClient
        .resolveA(name)
        .compose(
            a -> dnsClient.resolveAAAA(name).map(aaaa -> Stream.concat(aaaa.stream(), a.stream())))
        .recover(
            failure -> {
              LOGGER.warn("Failed to resolve '{}' to A/AAAA records", name, failure);
              return succeededFuture(Stream.of());
            });
  }

  Future<Stream<String>> resolveAll(List<String> names) {
    CompositeFuture composite = Future.all(names.stream().map(this::resolve).toList());
    return composite.map(
        c ->
            IntStream.range(0, c.size())
                .mapToObj(c::resultAt)
                .map(
                    e -> {
                      @SuppressWarnings({"UnnecessaryLocalVariable", "unchecked"})
                      Stream<String> casted = (Stream<String>) e;
                      return casted;
                    })
                .reduce(Stream::concat)
                .orElse(Stream.empty()));
  }
}
