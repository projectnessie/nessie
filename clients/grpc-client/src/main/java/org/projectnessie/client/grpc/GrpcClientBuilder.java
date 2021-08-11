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
package org.projectnessie.client.grpc;

import com.google.common.base.Preconditions;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.net.URI;
import java.util.function.Function;
import org.projectnessie.client.NessieClientBuilder;
import org.projectnessie.client.api.NessieApi;
import org.projectnessie.client.api.NessieApiVersion;
import org.projectnessie.client.auth.NessieAuthentication;

/**
 * A builder class that creates a {@link NessieGrpcClient} via {@link GrpcClientBuilder#builder()}.
 */
public class GrpcClientBuilder implements NessieClientBuilder<GrpcClientBuilder> {

  private URI endpoint;
  private ManagedChannel channel;
  /**
   * Returns a new {@link GrpcClientBuilder} instance.
   *
   * @return A new {@link GrpcClientBuilder} instance.
   */
  public static GrpcClientBuilder builder() {
    return new GrpcClientBuilder();
  }

  /**
   * Sets the {@link ManagedChannel} to use when connecting to the gRPC server.
   *
   * @param channel The {@link ManagedChannel} to use when connecting to the gRPC server.
   * @return {@code this}
   */
  public GrpcClientBuilder withChannel(ManagedChannel channel) {
    this.channel = channel;
    return this;
  }

  @Override
  public GrpcClientBuilder fromSystemProperties() {
    throw new UnsupportedOperationException("fromSystemProperties is not supported");
  }

  @Override
  public GrpcClientBuilder fromConfig(Function<String, String> configuration) {
    throw new UnsupportedOperationException("fromConfig is not supported");
  }

  @Override
  public GrpcClientBuilder withAuthenticationFromConfig(Function<String, String> configuration) {
    throw new UnsupportedOperationException("withAuthenticationFromConfig is not supported");
  }

  @Override
  public GrpcClientBuilder withAuthentication(NessieAuthentication authentication) {
    throw new UnsupportedOperationException("withAuthentication is not supported");
  }

  @Override
  public GrpcClientBuilder withUri(URI uri) {
    this.endpoint = uri;
    return this;
  }

  @Override
  public GrpcClientBuilder withUri(String uri) {
    return withUri(URI.create(uri));
  }

  @Override
  public <API extends NessieApi> API build(NessieApiVersion apiVersion, Class<API> apiContract) {
    NessieGrpcClient client = build();
    API api;
    if (apiVersion == NessieApiVersion.V_1) {
      api = (API) client;
    } else {
      throw new IllegalArgumentException(
          String.format("API version %s not supported.", apiVersion.name()));
    }
    if (!apiContract.isAssignableFrom(api.getClass())) {
      throw new IllegalArgumentException(
          String.format(
              "API version %s not supported with incompatible interface '%s' (not assignable from '%s').",
              apiVersion.name(), apiContract.getName(), api.getClass().getName()));
    }
    return api;
  }

  /**
   * Builds a new {@link NessieGrpcClient}.
   *
   * @return A new {@link NessieGrpcClient}.
   */
  public NessieGrpcClient build() {
    Preconditions.checkArgument(
        null != endpoint || null != channel, "channel or endpoint must be non-null");

    ManagedChannel c =
        null == channel
            ? ManagedChannelBuilder.forTarget(endpoint.toString()).usePlaintext().build()
            : channel;
    return new NessieGrpcClient(
        c, new GrpcConfigApi(c), new GrpcContentsApi(c), new GrpcTreeApi(c), endpoint);
  }
}
