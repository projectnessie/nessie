/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.server.catalog;

import static org.assertj.core.api.Assertions.assertThat;

import jakarta.ws.rs.core.UriInfo;
import java.net.URI;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.projectnessie.catalog.service.config.CatalogConfig;

class TestExternalBaseUriImpl {

  @Test
  void testBaseUri() {
    URI base1 = URI.create("http://localhost:1234");
    URI base2 = URI.create("https://example.com/path");
    UriInfo uriInfo = Mockito.mock(UriInfo.class);
    CatalogConfig config = Mockito.mock(CatalogConfig.class);
    ExternalBaseUriImpl impl = new ExternalBaseUriImpl(uriInfo, config);

    Mockito.when(uriInfo.getBaseUri()).thenReturn(base1);

    Mockito.when(config.externalBaseUri()).thenReturn(Optional.empty());
    assertThat(impl.externalBaseURI()).isEqualTo(base1);

    Mockito.when(config.externalBaseUri()).thenReturn(Optional.of(base2));
    assertThat(impl.externalBaseURI()).isEqualTo(base2);
  }
}
