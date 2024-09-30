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

public class BigTableContainerLifecycleManager
    extends AbstractContainerLifecycleManager<GenericContainer<?>> {

  public static final int BIGTABLE_PORT = 8086;

  @Target(ElementType.FIELD)
  @Retention(RetentionPolicy.RUNTIME)
  public @interface BigTableHost {}

  @SuppressWarnings("resource")
  @Override
  protected GenericContainer<?> createContainer() {
    return new GenericContainer<>(dockerImage("bigtable"))
        .withExposedPorts(BIGTABLE_PORT)
        .withCommand(
            "gcloud",
            "beta",
            "emulators",
            "bigtable",
            "start",
            "--verbosity=info",
            "--host-port=0.0.0.0:" + BIGTABLE_PORT)
        .waitingFor(Wait.forLogMessage(".*Bigtable emulator running.*", 1));
  }

  @Override
  public void inject(TestInjector testInjector) {
    testInjector.injectIntoFields(
        getInDockerIpAddress(),
        new MatchesType(String.class).and(new Annotated(BigTableHost.class)));
  }
}
