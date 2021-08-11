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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.projectnessie.grpc.ProtoUtil.fromProto;
import static org.projectnessie.grpc.ProtoUtil.refToProto;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.net.URI;
import org.junit.Rule;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.projectnessie.api.grpc.ConfigServiceGrpc.ConfigServiceImplBase;
import org.projectnessie.api.grpc.Contents;
import org.projectnessie.api.grpc.ContentsRequest;
import org.projectnessie.api.grpc.ContentsServiceGrpc.ContentsServiceImplBase;
import org.projectnessie.api.grpc.ContentsWithKey;
import org.projectnessie.api.grpc.Empty;
import org.projectnessie.api.grpc.GetAllReferencesResponse;
import org.projectnessie.api.grpc.IcebergTable;
import org.projectnessie.api.grpc.MultipleContentsRequest;
import org.projectnessie.api.grpc.MultipleContentsResponse;
import org.projectnessie.api.grpc.NessieConfiguration;
import org.projectnessie.api.grpc.TreeServiceGrpc.TreeServiceImplBase;
import org.projectnessie.client.api.NessieApi;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.model.Branch;
import org.projectnessie.model.ContentsKey;
import org.projectnessie.model.Reference;

public class TestNessieGrpcClient {

  private static final String GRPC_SERVER_NAME = "grpc-test-server";
  private static final Contents ICEBERG_TABLE =
      Contents.newBuilder().setIceberg(IcebergTable.newBuilder().build()).build();
  private static final String REF_NAME = "test-main";
  private static final Reference REF = Branch.of(REF_NAME, null);
  /** Manages automatic graceful shutdown for the registered servers and channels. */
  @Rule public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  /** Just a dummy implementation of a gRPC service to simulate a client call. */
  private final TreeServiceImplBase treeService =
      new TreeServiceImplBase() {
        @Override
        public void getAllReferences(
            Empty request, StreamObserver<GetAllReferencesResponse> responseObserver) {
          responseObserver.onNext(
              GetAllReferencesResponse.newBuilder().addReference(refToProto(REF)).build());
          responseObserver.onCompleted();
        }

        @Override
        public void getDefaultBranch(
            Empty request, StreamObserver<org.projectnessie.api.grpc.Reference> responseObserver) {
          responseObserver.onNext(
              org.projectnessie.api.grpc.Reference.newBuilder()
                  .setBranch(
                      org.projectnessie.api.grpc.Branch.newBuilder().setName(REF_NAME).build())
                  .build());
          responseObserver.onCompleted();
        }
      };

  /** Just a dummy implementation of a gRPC service to simulate a client call. */
  private final ContentsServiceImplBase contentsService =
      new ContentsServiceImplBase() {
        @Override
        public void getContents(
            ContentsRequest request, StreamObserver<Contents> responseObserver) {
          responseObserver.onNext(ICEBERG_TABLE);
          responseObserver.onCompleted();
        }

        @Override
        public void getMultipleContents(
            MultipleContentsRequest request,
            StreamObserver<MultipleContentsResponse> responseObserver) {
          responseObserver.onNext(
              MultipleContentsResponse.newBuilder()
                  .addContentsWithKey(
                      ContentsWithKey.newBuilder().setContents(ICEBERG_TABLE).build())
                  .build());
          responseObserver.onCompleted();
        }
      };

  /** Just a dummy implementation of a gRPC service to simulate a client call. */
  private final ConfigServiceImplBase configService =
      new ConfigServiceImplBase() {
        @Override
        public void getConfig(Empty request, StreamObserver<NessieConfiguration> responseObserver) {
          responseObserver.onNext(
              NessieConfiguration.newBuilder().setDefaultBranch(REF_NAME).build());
          responseObserver.onCompleted();
        }
      };

  interface IncompatibleApiInterface extends NessieApi {}

  @Test
  void testIncompatibleApiInterface() {
    assertThatThrownBy(
            () ->
                GrpcClientBuilder.builder()
                    .withChannel(Mockito.mock(ManagedChannel.class))
                    .build(IncompatibleApiInterface.class))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "API version org.projectnessie.client.grpc.TestNessieGrpcClient$IncompatibleApiInterface is not supported.");
  }

  @Test
  public void testNulls() {
    assertThatThrownBy(
            () ->
                GrpcClientBuilder.builder()
                    .withUri((URI) null)
                    .withChannel(null)
                    .build(NessieApiV1.class))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("channel or endpoint must be non-null");
    assertThatThrownBy(() -> GrpcClientBuilder.builder().withChannel(null).build(NessieApiV1.class))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("channel or endpoint must be non-null");
    assertThatThrownBy(
            () -> GrpcClientBuilder.builder().withUri((URI) null).build(NessieApiV1.class))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("channel or endpoint must be non-null");
  }

  @Test
  public void testApiCalls() throws IOException {
    ServiceWithChannel serviceWithChannel = startGrpcServer();
    assertThat(serviceWithChannel.server.getServices()).hasSize(3);

    NessieApiV1 api =
        GrpcClientBuilder.builder()
            .withChannel(serviceWithChannel.channel)
            .build(NessieApiV1.class);
    assertThat(api.getConfig().getDefaultBranch()).isEqualTo(REF_NAME);
    assertThat(api.getDefaultBranch().getName()).isEqualTo(REF_NAME);
    assertThat(api.getAllReferences().get()).containsExactly(REF);
    assertThat(api.getContents().key(ContentsKey.of("test")).refName(REF_NAME).get())
        .containsValue(fromProto(ICEBERG_TABLE));
  }

  private static class ServiceWithChannel {
    private final Server server;
    private final ManagedChannel channel;

    public ServiceWithChannel(Server server, ManagedChannel channel) {
      this.server = server;
      this.channel = channel;
    }
  }

  private ServiceWithChannel startGrpcServer() throws IOException {
    Server server =
        InProcessServerBuilder.forName(GRPC_SERVER_NAME)
            .directExecutor()
            .addService(treeService)
            .addService(configService)
            .addService(contentsService)
            .build();
    grpcCleanup.register(server.start());
    ManagedChannel channel = InProcessChannelBuilder.forName(GRPC_SERVER_NAME).build();
    grpcCleanup.register(channel);
    return new ServiceWithChannel(server, channel);
  }
}
