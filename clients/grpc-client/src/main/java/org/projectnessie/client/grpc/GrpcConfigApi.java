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

import static org.projectnessie.grpc.ProtoUtil.fromProto;

import io.grpc.Channel;
import org.projectnessie.api.ConfigApi;
import org.projectnessie.api.grpc.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.projectnessie.api.grpc.Empty;
import org.projectnessie.model.NessieConfiguration;

/** The gRPC client-side implementation of the {@link ConfigApi}. */
public class GrpcConfigApi implements ConfigApi {
  private final ConfigServiceBlockingStub stub;

  public GrpcConfigApi(Channel channel) {
    this.stub = org.projectnessie.api.grpc.ConfigServiceGrpc.newBlockingStub(channel);
  }

  @Override
  public NessieConfiguration getConfig() {
    return fromProto(stub.getConfig(Empty.newBuilder().build()));
  }
}
