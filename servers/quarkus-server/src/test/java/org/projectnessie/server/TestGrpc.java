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
package org.projectnessie.server;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import java.net.URI;
import org.junit.jupiter.api.BeforeEach;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.client.api.NessieApiVersion;
import org.projectnessie.client.grpc.GrpcClientBuilder;
import org.projectnessie.jaxrs.AbstractTestRest;

@QuarkusTest
@TestProfile(value = TestGrpc.GrpcTestProfile.class)
class TestGrpc extends AbstractTestRest {

  @Override
  @BeforeEach
  public void setUp() throws Exception {
    super.init(
        GrpcClientBuilder.builder()
            .withUri(new URI("dns", null, "/localhost", 9001, null, null, null))
            .build(NessieApiVersion.V_1, NessieApiV1.class),
        null);
    super.setUp();
  }

  /**
   * The purpose of this is to force a restart of the Quarkus server, because {@link TestRest} and
   * {@link TestGrpc} effectively create the same branches.
   */
  public static class GrpcTestProfile implements QuarkusTestProfile {}
}
