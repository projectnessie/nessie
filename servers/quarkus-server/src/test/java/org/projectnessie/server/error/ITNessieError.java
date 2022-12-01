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
package org.projectnessie.server.error;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.quarkus.test.junit.TestProfile;
import java.net.URI;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.client.ext.NessieApiVersion;
import org.projectnessie.client.ext.NessieApiVersions;
import org.projectnessie.client.ext.NessieClientFactory;
import org.projectnessie.client.ext.NessieClientUri;
import org.projectnessie.client.http.HttpClient;
import org.projectnessie.client.rest.NessieHttpResponseFilter;
import org.projectnessie.error.NessieBadRequestException;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.Operation;
import org.projectnessie.model.Reference;
import org.projectnessie.quarkus.tests.profiles.QuarkusTestProfileInmemory;
import org.projectnessie.server.QuarkusNessieClientResolver;

/**
 * Rudimentary version of {@link TestNessieError}, because we cannot dynamically add beans and
 * REST-endpoints declared in test-source to the native-image-binary; so this test checks just some
 * very basic validation functionality.
 */
@QuarkusIntegrationTest
@TestProfile(
    QuarkusTestProfileInmemory.class) // use the QuarkusTestProfileInmemory, as it can be reused
@ExtendWith(QuarkusNessieClientResolver.class)
@NessieApiVersions
public class ITNessieError {

  @Test
  @NessieApiVersions(versions = NessieApiVersion.V1)
  void testNullParamViolationV1(NessieClientFactory client) {
    try (NessieApiV1 api = client.make()) {
      ContentKey k = ContentKey.of("a");
      IcebergTable t = IcebergTable.of("path1", 42, 42, 42, 42);
      assertEquals(
          "Bad Request (HTTP/400): commitMultipleOperations.expectedHash: must not be null",
          assertThrows(
                  NessieBadRequestException.class,
                  () ->
                      api.commitMultipleOperations()
                          .branchName("branchName")
                          .operation(Operation.Put.of(k, t))
                          .commitMeta(CommitMeta.fromMessage("message"))
                          .commit())
              .getMessage());
    }
  }

  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  void testNullParamViolationV2(@NessieClientUri URI uri) {
    ObjectMapper mapper =
        new ObjectMapper()
            .enable(SerializationFeature.INDENT_OUTPUT)
            .disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
    HttpClient client =
        HttpClient.builder()
            .setBaseUri(uri)
            .setObjectMapper(mapper)
            .addResponseFilter(new NessieHttpResponseFilter(mapper))
            .build();

    assertThatThrownBy(
            () ->
                client
                    .newRequest()
                    .path("trees")
                    .queryParam("type", Reference.ReferenceType.BRANCH.name())
                    .post(null))
        .isInstanceOf(NessieBadRequestException.class)
        .hasMessage("Bad Request (HTTP/400): createReference.name: must not be null");
  }
}
