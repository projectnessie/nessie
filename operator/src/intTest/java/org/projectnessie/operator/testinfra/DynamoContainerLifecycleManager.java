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
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

public class DynamoContainerLifecycleManager
    extends AbstractContainerLifecycleManager<GenericContainer<?>> {

  public static final int DYNAMODB_PORT = 8000;

  @Target(ElementType.FIELD)
  @Retention(RetentionPolicy.RUNTIME)
  public @interface DynamoEndpoint {}

  @SuppressWarnings("resource")
  @Override
  protected GenericContainer<?> createContainer() {
    return new GenericContainer<>(dockerImage("dynamo"))
        .withExposedPorts(DYNAMODB_PORT)
        .withCommand("-jar", "DynamoDBLocal.jar", "-inMemory", "-sharedDb")
        .waitingFor(Wait.forLogMessage(".*Initializing DynamoDB Local.*", 1));
  }

  @Override
  public void inject(TestInjector testInjector) {
    super.inject(testInjector);
    String endpoint = String.format("http://%s:%d", getInDockerIpAddress(), DYNAMODB_PORT);
    testInjector.injectIntoFields(
        endpoint, new MatchesType(String.class).and(new Annotated(DynamoEndpoint.class)));
  }
}
