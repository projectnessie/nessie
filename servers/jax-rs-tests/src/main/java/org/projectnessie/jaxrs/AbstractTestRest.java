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

/**
 * Base test class for Nessie REST APIs.
 *
 * <h2>Base class organization</h2>
 *
 * (See below for reasons)
 *
 * <p>This abstract test class represents the base test class for Jersey based tests and tests in
 * Quarkus.
 *
 * <p>Every base class must extend each other, as a rule of thumb, maintain alphabetical order.
 *
 * <p>Another rule of thumb: when a class reaches ~ 500 LoC, split it, when it makes sense.
 *
 * <h2>Reasons for the "chain of base classes"</h2>
 *
 * <p>Tests have been moved to base classes so that this class is not a "monster of couple thousand
 * lines of code".
 *
 * <p>Splitting {@code AbstractTestRest} into separate classes and including those via
 * {@code @Nested} with plain JUnit 5 works fine. But it does not work as a {@code @QuarkusTest} for
 * several reasons.
 *
 * <p>Using {@code @Nested} test classes like this does not work, because Quarkus considers the
 * non-static inner classes as a separate test classes and fails horribly, mostly complaining that
 * the inner test class is not in one of the expected class folders.
 *
 * <p>Even worse is that the whole Quarkus machinery is started for each nested test class and not
 * just for the actual test class.
 *
 * <p>As an alternative it felt doable to refactor the former inner classes to interfaces (so test
 * methods become default methods), but that does not work as well due to an NPE, when {@code
 * QuarkusSecurityTestExtension.beforeEach} tries to find the {@code @TestSecurity} annotation - it
 * just asserts whether the current {@code Class} is {@code != Object.class}, but {@code
 * Class.getSuperclass()} returns {@code null} for interfaces - hence the NPE there.
 *
 * <p>The "solution" here is to keep the separate classes but let each extend another - so different
 * groups of tests are kept in a single class.
 */
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
