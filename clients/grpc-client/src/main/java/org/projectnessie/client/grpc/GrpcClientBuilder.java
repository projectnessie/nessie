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

/**
 * A builder class that creates a {@link NessieGrpcClient} via {@link GrpcClientBuilder#builder()}.
 */
public class GrpcClientBuilder {

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
   * Sets the Nessie gRPC Server endpoint.
   *
   * @param endpoint The gRPC Server endpoint
   * @return {@code this}
   */
  public GrpcClientBuilder withEndpoint(URI endpoint) {
    this.endpoint = endpoint;
    return this;
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
