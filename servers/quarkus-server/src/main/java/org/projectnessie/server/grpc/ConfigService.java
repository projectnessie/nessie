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
package org.projectnessie.server.grpc;

import static org.projectnessie.grpc.ProtoUtil.fromProto;

import io.grpc.stub.StreamObserver;
import io.quarkus.grpc.GrpcService;
import javax.inject.Inject;
import org.projectnessie.api.grpc.ConfigServiceGrpc;
import org.projectnessie.api.grpc.Empty;
import org.projectnessie.api.grpc.NessieConfiguration;
import org.projectnessie.client.grpc.GrpcExceptionMapper;
import org.projectnessie.services.rest.ConfigResource;

/** The gRPC service implementation for the Config-API. */
@GrpcService
public class ConfigService extends ConfigServiceGrpc.ConfigServiceImplBase {

  private final ConfigResource configResource;

  @Inject
  public ConfigService(ConfigResource configResource) {
    this.configResource = configResource;
  }

  @Override
  public void getConfig(Empty request, StreamObserver<NessieConfiguration> observer) {
    try {
      observer.onNext(fromProto(configResource.getConfig()));
      observer.onCompleted();
    } catch (Exception e) {
      observer.onError(GrpcExceptionMapper.toProto(e));
    }
  }
}
