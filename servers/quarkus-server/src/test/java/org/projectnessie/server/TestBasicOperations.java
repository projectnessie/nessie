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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.net.HttpURLConnection;
import java.net.URI;
import org.junit.jupiter.api.Test;
import org.projectnessie.quarkus.config.QuarkusStoreConfig;
import org.projectnessie.quarkus.tests.profiles.QuarkusTestProfilePersistInmemory;

@QuarkusTest
@TestProfile(QuarkusTestProfilePersistInmemory.class)
public class TestBasicOperations extends AbstractTestBasicOperations {

  @Inject QuarkusStoreConfig storeConfig;

  @Test
  public void receiveCacheInvalidationSmoke() throws Exception {
    int managementPort = Integer.getInteger("quarkus.management.port");
    URI uri =
        new URI(
            "http",
            null,
            "127.0.0.1",
            managementPort,
            storeConfig.cacheInvalidationUri(),
            null,
            null);

    HttpURLConnection conn = (HttpURLConnection) uri.toURL().openConnection();
    conn.setRequestMethod("POST");
    conn.connect();
    int code = conn.getResponseCode();
    String msg = conn.getResponseMessage();
    assertThat(tuple(code, msg)).isEqualTo(tuple(400, "Invalid token"));
  }
}
