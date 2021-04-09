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
package org.projectnessie.quarkus.gradle;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.IOException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.projectnessie.client.NessieClient;
import org.projectnessie.model.Branch;

/**
 * This is not a test for the plugin itself, this is a test that is run BY the test for the plugin.
 */
class TestSimulatingTestUsingThePlugin {
  @Test
  void pingNessie() throws Exception {
    String port = System.getProperty("quarkus.http.test-port");
    assertNotNull(port);

    String uri = String.format("http://localhost:%s/api/v1", port);

    NessieClient client = NessieClient.builder().withUri(uri).build();
    // Just some simple REST request to verify that Nessie is started - no fancy interactions w/ Nessie
    client.getConfigApi().getConfig();

    // We have seen that HTTP/POST requests can fail with conflicting dependencies
    client.getTreeApi().createReference(Branch.of("foo-" + System.nanoTime(), null));
  }
}
