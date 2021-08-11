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
import java.net.URI;
import org.projectnessie.api.ConfigApi;
import org.projectnessie.api.ContentsApi;
import org.projectnessie.api.TreeApi;
import org.projectnessie.client.NessieClient;

/** The Nessie gRPC Client implementation. */
class NessieGrpcClient implements NessieClient {

  private final ManagedChannel channel;
  private final ConfigApi configApi;
  private final ContentsApi contentsApi;
  private final TreeApi treeApi;

  public NessieGrpcClient(
      ManagedChannel channel, ConfigApi configApi, ContentsApi contentsApi, TreeApi treeApi) {
    this.channel = channel;
    this.configApi = configApi;
    this.contentsApi = contentsApi;
    this.treeApi = treeApi;
  }

  @Override
  public void close() {
    if (null != channel) {
      channel.shutdown();
    }
  }

  @Override
  public TreeApi getTreeApi() {
    return treeApi;
  }

  @Override
  public ContentsApi getContentsApi() {
    return contentsApi;
  }

  @Override
  public ConfigApi getConfigApi() {
    return configApi;
  }

  @Override
  public String getOwner() {
    throw new UnsupportedOperationException("getOwner() is currently not supported");
  }

  @Override
  public String getRepo() {
    throw new UnsupportedOperationException("getRepo() is currently not supported");
  }

  @Override
  public URI getUri() {
    throw new UnsupportedOperationException("getUri() is currently not supported");
  }
}
