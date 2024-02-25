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
package org.projectnessie.operator.testinfra;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager.TestInjector.Annotated;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager.TestInjector.MatchesType;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.intellij.lang.annotations.Language;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.images.builder.Transferable;

public class MongoContainerLifecycleManager
    extends AbstractContainerLifecycleManager<MongoDBContainer> {

  public static final String DATABASE_NAME = "nessie";
  public static final int MONGO_PORT = 27017;

  @Language("JavaScript")
  private static final String MONGO_INIT_JS =
      """
      db.createUser({user: "nessie", pwd: "nessie", roles: [{role: "readWrite", db: "nessie"}]});
      """;

  @Language("Shell Script")
  private static final String ENTRYPOINT_SH =
      """
      openssl rand -base64 756 > /etc/mongodb.key && \
      chmod 400 /etc/mongodb.key && \
      chown 999:999 /etc/mongodb.key && \
      chown 999:999 /docker-entrypoint-initdb.d/mongo-init.js && \
      docker-entrypoint.sh --replSet docker-rs --keyFile /etc/mongodb.key""";

  @Target(ElementType.FIELD)
  @Retention(RetentionPolicy.RUNTIME)
  public @interface MongoConnectionString {}

  @SuppressWarnings("resource")
  @Override
  protected MongoDBContainer createContainer() {
    return new MongoDBContainer(ContainerImages.MONGO.image())
        .withEnv("MONGO_INITDB_DATABASE", "nessie")
        .withCopyToContainer(
            Transferable.of(MONGO_INIT_JS), "/docker-entrypoint-initdb.d/mongo-init.js")
        .withCommand("/bin/sh", "-c", ENTRYPOINT_SH);
  }

  @Override
  public void inject(TestInjector testInjector) {
    super.inject(testInjector);
    String connectionString = String.format("mongodb://%s:%d", inDockerIpAddress, MONGO_PORT);
    testInjector.injectIntoFields(
        connectionString,
        new MatchesType(String.class).and(new Annotated(MongoConnectionString.class)));
  }
}
