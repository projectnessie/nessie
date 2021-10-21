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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.quarkus.test.junit.NativeImageTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.client.http.HttpClientBuilder;
import org.projectnessie.client.rest.NessieBadRequestException;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.ContentsKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.Operation.Put;

/**
 * Rudimentary version of {@link TestNessieError}, because we cannot dynamically add beans and
 * REST-endpoints declared in test-source to the native-image-binary; so this test checks just some
 * very basic validation functionality.
 */
@NativeImageTest
public class ITNativeNessieError {

  private NessieApiV1 api;

  @BeforeEach
  void init() {
    api =
        HttpClientBuilder.builder()
            .withUri("http://localhost:19121/api/v1")
            .build(NessieApiV1.class);
  }

  @Test
  void testNullParamViolation() {
    ContentsKey k = ContentsKey.of("a");
    IcebergTable t = IcebergTable.of("path1", "x");
    assertEquals(
        "Bad Request (HTTP/400): commitMultipleOperations.hash: must not be null",
        assertThrows(
                NessieBadRequestException.class,
                () ->
                    api.commitMultipleOperations()
                        .branchName("branchName")
                        .operation(Put.of(k, t))
                        .commitMeta(CommitMeta.fromMessage("message"))
                        .commit())
            .getMessage());
  }
}
