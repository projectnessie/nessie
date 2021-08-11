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

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.projectnessie.client.NessieClient;

public class GrpcClientBuilder {

  private String endpoint;

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
  public GrpcClientBuilder withEndpoint(String endpoint) {
    this.endpoint = endpoint;
    return this;
  }

  /**
   * Builds a new gRPC {@link NessieClient}.
   *
   * @return A new gRPC {@link NessieClient}.
   */
  public NessieClient build() {
    ManagedChannel channel = ManagedChannelBuilder.forTarget(endpoint).usePlaintext().build();
    GrpcConfigApi configApi = new GrpcConfigApi(channel);
    GrpcContentsApi contentsApi = new GrpcContentsApi(channel);
    GrpcTreeApi treeApi = new GrpcTreeApi(channel);

    return new NessieGrpcClient(channel, configApi, contentsApi, treeApi);
  }
}
