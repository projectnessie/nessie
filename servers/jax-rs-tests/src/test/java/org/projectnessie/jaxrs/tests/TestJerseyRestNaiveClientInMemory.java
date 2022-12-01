/*
 * Copyright (C) 2022 Dremio
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
package org.projectnessie.jaxrs.tests;

import static org.assertj.core.api.Assertions.assertThat;
import static org.projectnessie.client.http.impl.HttpUtils.HEADER_ACCEPT;

import org.junit.jupiter.api.AfterEach;
import org.projectnessie.client.NessieClientBuilder;
import org.projectnessie.client.ext.NessieApiVersions;
import org.projectnessie.client.ext.NessieClientCustomizer;
import org.projectnessie.client.http.HttpAuthentication;
import org.projectnessie.client.http.RequestFilter;
import org.projectnessie.versioned.persist.inmem.InmemoryDatabaseAdapterFactory;
import org.projectnessie.versioned.persist.inmem.InmemoryTestConnectionProviderSource;
import org.projectnessie.versioned.persist.tests.extension.NessieDbAdapterName;
import org.projectnessie.versioned.persist.tests.extension.NessieExternalDatabase;

/**
 * This tests simulates a naive REST client that does not provide an {@code Accept} HTTP header.
 *
 * <p>This test serves two main purposes:
 *
 * <ol>
 *   <li>To ensure that all (tested) API endpoint methods have proper {@code @Produces} annotations.
 *   <li>To validate user experience for command line tools like {@code curl}, where users may
 *       inadvertently omit the {@code Accept} header parameter.
 * </ol>
 *
 * <p>It is not necessary to run this test for all backends as it tests only the surface area of
 * HTTP endpoints. Running with the in-memory database adapter is sufficient.
 */
@NessieDbAdapterName(InmemoryDatabaseAdapterFactory.NAME)
@NessieExternalDatabase(InmemoryTestConnectionProviderSource.class)
@NessieApiVersions
class TestJerseyRestNaiveClientInMemory extends AbstractTestDatabaseAdapterRest
    implements NessieClientCustomizer {

  private boolean headersProcessed;

  @Override
  public NessieClientBuilder<?> configure(NessieClientBuilder<?> builder) {
    // Intentionally remove the `Accept` header from requests.
    // Service endpoints should declare the content type for their return values,
    // which should allow the Web Container to properly format output even in the absence
    // of `Accept` HTTP headers.
    headersProcessed = false;
    RequestFilter noAcceptFilter =
        context -> {
          headersProcessed = true;
          context.removeHeader(HEADER_ACCEPT);
        };

    // Abuse the authentication callback a bit to inject the noAcceptFilter into the java client.
    return builder.withAuthentication(
        (HttpAuthentication) client -> client.addRequestFilter(noAcceptFilter));
  }

  @AfterEach
  void ensureHeadersProcessed() {
    assertThat(headersProcessed).isTrue();
  }
}
