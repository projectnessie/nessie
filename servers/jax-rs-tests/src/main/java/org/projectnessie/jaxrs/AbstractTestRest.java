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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.net.URI;
import java.util.Locale;
import javax.annotation.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.client.http.HttpClient;
import org.projectnessie.client.http.HttpClientBuilder;
import org.projectnessie.client.rest.NessieHttpResponseFilter;
import org.projectnessie.model.Branch;
import org.projectnessie.model.Reference;
import org.projectnessie.model.Tag;

public abstract class AbstractTestRest extends AbstractRestRefLog {

  private NessieApiV1 api;
  private HttpClient httpClient;

  static {
    // Note: REST tests validate some locale-specific error messages, but expect on the messages to
    // be in ENGLISH. However, the JRE's startup classes (in particular class loaders) may cause the
    // default Locale to be initialized before Maven is able to override the user.language system
    // property. Therefore, we explicitly set the default Locale to ENGLISH here to match tests'
    // expectations.
    Locale.setDefault(Locale.ENGLISH);
  }

  protected void init(URI uri) {
    NessieApiV1 api = HttpClientBuilder.builder().withUri(uri).build(NessieApiV1.class);

    ObjectMapper mapper =
        new ObjectMapper()
            .enable(SerializationFeature.INDENT_OUTPUT)
            .disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
    HttpClient.Builder httpClient = HttpClient.builder().setBaseUri(uri).setObjectMapper(mapper);
    httpClient.addResponseFilter(new NessieHttpResponseFilter(mapper));

    init(api, httpClient);
  }

  protected void init(NessieApiV1 api, @Nullable HttpClient.Builder httpClient) {
    this.api = api;
    this.httpClient = httpClient != null ? httpClient.build() : null;
  }

  @BeforeEach
  public void setUp() {
    init(URI.create("http://localhost:19121/api/v1"));
  }

  @AfterEach
  public void tearDown() throws Exception {
    Branch defaultBranch = api.getDefaultBranch();
    for (Reference ref : api.getAllReferences().get().getReferences()) {
      if (ref.getName().equals(defaultBranch.getName())) {
        continue;
      }
      if (ref instanceof Branch) {
        api.deleteBranch().branch((Branch) ref).delete();
      } else if (ref instanceof Tag) {
        api.deleteTag().tag((Tag) ref).delete();
      }
    }

    api.close();
  }

  @Override
  public NessieApiV1 getApi() {
    return api;
  }

  @Override
  public HttpClient getHttpClient() {
    return httpClient;
  }
}
