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

import java.util.Optional;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.enterprise.inject.Default;
import javax.enterprise.inject.spi.AfterBeanDiscovery;
import javax.enterprise.inject.spi.BeanManager;
import javax.enterprise.inject.spi.Extension;
import javax.enterprise.util.TypeLiteral;
import javax.ws.rs.core.Application;

import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Contents;
import org.projectnessie.server.store.TableCommitMetaStoreWorker;
import org.projectnessie.services.config.ServerConfig;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.ReferenceAlreadyExistsException;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.memory.InMemoryVersionStore;

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
    weld.addExtension(new Extension() {
      @SuppressWarnings("unused")
      public void afterBeanDiscovery(@Observes AfterBeanDiscovery abd, BeanManager bm) {
        final ServerConfig serverConfig = new ServerConfig() {

          @Override
          public boolean sendStacktraceToClient() {
            return false;
          }

          @Override
          public String getDefaultBranch() {
            return "main";
          }
        };

        final TableCommitMetaStoreWorker storeWorker = new TableCommitMetaStoreWorker();
        final VersionStore<Contents, CommitMeta, Contents.Type> store = InMemoryVersionStore
            .<Contents, CommitMeta, Contents.Type> builder()
            .valueSerializer(storeWorker.getValueSerializer())
            .metadataSerializer(storeWorker.getMetadataSerializer())
            .build();

        try {
          store.create(BranchName.of(serverConfig.getDefaultBranch()), Optional.empty());
        } catch (ReferenceNotFoundException | ReferenceAlreadyExistsException e) {
          throw new RuntimeException(e);
        }

        abd.addBean()
          .addType(ServerConfig.class)
          .addQualifier(Default.Literal.INSTANCE)
          .scope(ApplicationScoped.class)
          .produceWith(i -> serverConfig);
        abd.addBean()
          .addType(new TypeLiteral<VersionStore<Contents, CommitMeta, Contents.Type>>() {})
          .addQualifier(Default.Literal.INSTANCE)
          .scope(ApplicationScoped.class)
          .produceWith(i -> store);
      }
    });
    final WeldContainer container = weld.initialize();

    jerseyTest = new JerseyTest() {
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
//        config.register(new AbstractBinder() {
//
//          @Override protected void configure() {
//            bind(ValidationExceptionMapper.class).to(ExceptionMapper.class).ranked(10);
//          }
//        });
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
