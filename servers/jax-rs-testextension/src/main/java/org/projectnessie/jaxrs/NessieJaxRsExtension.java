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

import static java.nio.file.FileVisitResult.CONTINUE;
import static org.projectnessie.services.config.ServerConfigExtension.SERVER_CONFIG;

import java.io.IOException;
import java.net.URI;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import javax.ws.rs.core.Application;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.projectnessie.services.config.ServerConfigExtension;
import org.projectnessie.services.rest.ConfigResource;
import org.projectnessie.services.rest.ContentsKeyParamConverterProvider;
import org.projectnessie.services.rest.ContentsResource;
import org.projectnessie.services.rest.InstantParamConverterProvider;
import org.projectnessie.services.rest.NessieExceptionMapper;
import org.projectnessie.services.rest.TreeResource;
import org.projectnessie.services.rest.ValidationExceptionMapper;
import org.projectnessie.versioned.VersionStoreExtension;
import org.projectnessie.versioned.tiered.adapter.DatabaseAdapter;
import org.projectnessie.versioned.tiered.adapter.SystemPropertiesConfigurer;
import org.projectnessie.versioned.tiered.h2.H2DatabaseAdapterFactory;
import org.projectnessie.versioned.tiered.inmem.InmemoryDatabaseAdapterFactory;
import org.projectnessie.versioned.tiered.rocks.RocksDatabaseAdapterFactory;

/** A JUnit 5 extension that starts up Weld/JerseyTest. */
public class NessieJaxRsExtension implements BeforeAllCallback, AfterAllCallback {
  private Weld weld;
  private JerseyTest jerseyTest;
  private Path tempDir;

  private final Type type;

  public NessieJaxRsExtension() {
    this(Type.INMEMORY);
  }

  public NessieJaxRsExtension(Type type) {
    this.type = type;
  }

  public enum Type {
    INMEMORY,
    ROCKS,
    H2
  }

  @Override
  public void beforeAll(ExtensionContext extensionContext) throws Exception {
    tempDir = Files.createTempDirectory("nessie-jaxrs-ext");

    DatabaseAdapter databaseAdapter;
    switch (type) {
      case INMEMORY:
        databaseAdapter = buildInMemoryDatabaseAdapter();
        break;
      case ROCKS:
        databaseAdapter = buildRocksDatabaseAdapter();
        break;
      case H2:
        databaseAdapter = buildH2DatabaseAdapter();
        break;
      default:
        throw new IllegalStateException("type = " + type);
    }
    databaseAdapter.reinitializeRepo();

    weld = new Weld();
    // Let Weld scan all the resources to discover injection points and dependencies
    weld.addPackages(true, TreeResource.class);
    // Inject external beans
    weld.addExtension(new ServerConfigExtension());
    weld.addExtension(VersionStoreExtension.forDatabaseAdapter(databaseAdapter));
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
  }

  private DatabaseAdapter buildInMemoryDatabaseAdapter() {
    return new InmemoryDatabaseAdapterFactory()
        .newBuilder()
        .configure(c -> c.withDefaultBranch(SERVER_CONFIG.getDefaultBranch()))
        .configure(SystemPropertiesConfigurer::configureFromSystemProperties)
        .build();
  }

  private DatabaseAdapter buildRocksDatabaseAdapter() {
    return new RocksDatabaseAdapterFactory()
        .newBuilder()
        .configure(
            c ->
                c.withDbPath(tempDir.toString())
                    .withDefaultBranch(SERVER_CONFIG.getDefaultBranch()))
        .configure(SystemPropertiesConfigurer::configureFromSystemProperties)
        .build();
  }

  private DatabaseAdapter buildH2DatabaseAdapter() {
    return new H2DatabaseAdapterFactory()
        .newBuilder()
        .configure(c -> c.withDefaultBranch(SERVER_CONFIG.getDefaultBranch()))
        .configure(SystemPropertiesConfigurer::configureFromSystemProperties)
        .build();
  }

  @Override
  public void afterAll(ExtensionContext extensionContext) throws Exception {
    if (null != jerseyTest) jerseyTest.tearDown();
    if (null != weld) weld.shutdown();
    if (null != tempDir && Files.exists(tempDir)) {
      Files.walkFileTree(
          tempDir,
          new SimpleFileVisitor<Path>() {

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attributes) {
              return deleteAndContinue(file);
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) {
              return deleteAndContinue(dir);
            }

            private FileVisitResult deleteAndContinue(Path path) {
              try {
                Files.delete(path);
              } catch (NoSuchFileException ignore) {
                // ignore
              } catch (DirectoryNotEmptyException exception) {
                exception.printStackTrace();
              } catch (IOException exception) {
                makeWritableAndTryToDeleteAgain(path, exception);
              }
              return CONTINUE;
            }

            private void makeWritableAndTryToDeleteAgain(Path path, IOException exception) {
              try {
                tryToMakeParentDirsWritable(path);
                makeWritable(path);
                Files.delete(path);
              } catch (Exception suppressed) {
                exception.addSuppressed(suppressed);
              }
            }

            private void tryToMakeParentDirsWritable(Path path) {
              Path relativePath = tempDir.relativize(path);
              Path parent = tempDir;
              for (int i = 0; i < relativePath.getNameCount(); i++) {
                boolean writable = parent.toFile().setWritable(true);
                if (!writable) {
                  break;
                }
                parent = parent.resolve(relativePath.getName(i));
              }
            }

            private void makeWritable(Path path) {
              boolean writable = path.toFile().setWritable(true);
              if (!writable) {
                throw new RuntimeException("Attempt to make file '" + path + "' writable failed");
              }
            }
          });
    }
  }

  public URI getURI() {
    if (null == jerseyTest) {
      return null;
    }
    return jerseyTest.target().getUri();
  }
}
