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
package org.projectnessie.jaxrs;

import static org.assertj.core.api.Assumptions.assumeThat;
import static org.projectnessie.client.http.HttpUtils.HEADER_ACCEPT;

import java.net.URI;
import org.jetbrains.annotations.Nullable;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.client.http.HttpAuthentication;
import org.projectnessie.client.http.HttpClient;
import org.projectnessie.client.http.HttpClientBuilder;
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
class TestJerseyRestNaiveClientInMemory extends AbstractTestJerseyRest {

  @Override
  protected void init(NessieApiV1 api, @Nullable HttpClient.Builder httpClient, URI uri) {
    assumeThat(httpClient).isNotNull();

    // Intentionally remove the `Accept` header from requests.
    // Service endpoints should declare the content type for their return values,
    // which should allow the Web Container to properly format output even in the absence
    // of `Accept` HTTP headers.
    RequestFilter noAcceptFilter = context -> context.removeHeader(HEADER_ACCEPT);
    httpClient.addRequestFilter(noAcceptFilter);

    api =
        HttpClientBuilder.builder()
            // Abuse the authentication callback a bit to inject the noAcceptFilter into
            // the java client.
            .withAuthentication(
                (HttpAuthentication) client -> client.addRequestFilter(noAcceptFilter))
            .withUri(httpClient.getBaseUri())
            .build(NessieApiV1.class);

    super.init(api, httpClient, uri);
  }
}
