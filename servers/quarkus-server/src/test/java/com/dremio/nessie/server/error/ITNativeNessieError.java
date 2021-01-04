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
package com.dremio.nessie.server.error;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.dremio.nessie.api.ContentsApi;
import com.dremio.nessie.client.NessieClient;
import com.dremio.nessie.client.NessieClient.AuthType;
import com.dremio.nessie.client.rest.NessieBadRequestException;
import com.dremio.nessie.model.ContentsKey;
import com.dremio.nessie.model.IcebergTable;

import io.quarkus.test.junit.NativeImageTest;

/**
 * Rudimentary version of {@link TestNessieError}, because we cannot dynamically add beans
 * and REST-endpoints declared in test-source to the native-image-binary; so this test checks
 * just some very basic validation functionality.
 */
@NativeImageTest
public class ITNativeNessieError {

  private ContentsApi contents;

  @BeforeEach
  void init() {
    String path = "http://localhost:19121/api/v1";
    NessieClient client = new NessieClient(AuthType.NONE, path, null, null);
    contents = client.getContentsApi();
  }

  @Test
  void testNullParamViolation() {
    ContentsKey k = ContentsKey.of("a");
    IcebergTable t = IcebergTable.of("path1");
    assertEquals(
        "Bad Request (HTTP/400): setContents.hash: must not be null",
        assertThrows(
            NessieBadRequestException.class,
            () -> contents.setContents(k, "branchName", null, "message", t))
              .getMessage());
  }

}
