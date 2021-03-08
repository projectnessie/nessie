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
package org.projectnessie.services.rest;

import javax.ws.rs.core.Application;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.projectnessie.services.config.ServerConfigExtension;
import org.projectnessie.versioned.VersionStoreExtension;

public class TestJerseyRest extends AbstractTestRest {
  private Weld weld;
  private JerseyTest jerseyTest;

  @Override
  @BeforeEach
  public void setUp() throws Exception {
    weld = new Weld();
    // Let Weld scan all the resources to discover injection points and dependencies
    weld.addPackages(true, TreeResource.class);
    // Inject external beans
    weld.addExtension(new ServerConfigExtension());
    weld.addExtension(new VersionStoreExtension());
    final WeldContainer container = weld.initialize();

    jerseyTest =
        new JerseyTest() {
          @Override
          protected Application configure() {
            ResourceConfig config = new ResourceConfig();
            config.register(TreeResource.class);
            config.register(ContentsResource.class);
            config.register(ConfigResource.class);
            config.register(ContentsKeyParamConverterProvider.class);
            config.register(InstantParamConverterProvider.class);
            config.register(ValidationExceptionMapper.class, 10);
            config.register(NessieExceptionMapper.class);
            config.register(NessieJaxRsJsonParseExceptionMapper.class, 10);
            config.register(NessieJaxRsJsonMappingExceptionMapper.class, 10);
            return config;
          }
        };

    jerseyTest.setUp();
    init(jerseyTest.target().getUri());

    super.setUp();
  }

  @Override
  @AfterEach
  public void tearDown() throws Exception {
    super.tearDown();
    jerseyTest.tearDown();
    weld.shutdown();
  }
}
