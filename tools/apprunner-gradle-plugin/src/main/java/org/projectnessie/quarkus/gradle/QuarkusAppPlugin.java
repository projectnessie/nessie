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
package org.projectnessie.quarkus.gradle;

import org.gradle.api.Action;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.file.RegularFile;
import org.gradle.api.tasks.testing.Test;

@SuppressWarnings("Convert2Lambda") // Gradle complains when using lambdas (build-cache won't wonk)
public class QuarkusAppPlugin implements Plugin<Project> {

  static final String EXTENSION_NAME = "nessieQuarkusApp";

  /**
   * The configuration that contains the Quarkus server application as the only dependency.
   */
  static final String APP_CONFIG_NAME = "nessieQuarkusServer";

  @Override
  public void apply(Project target) {
    QuarkusAppExtension extension = target.getExtensions().create(EXTENSION_NAME, QuarkusAppExtension.class, target);

    Configuration appConfig = target.getConfigurations().create(APP_CONFIG_NAME)
        .setTransitive(false)
        .setDescription("References the Nessie-Quarkus server dependency, only a single dependency allowed.");

    // Cannot use the task name "test" here, because the "test" task might not have been registered yet.
    // This `withType(Test.class...)` construct will configure any current and future task of type `Test`.
    target.getTasks().withType(Test.class, new Action<Test>() {
      @SuppressWarnings("UnstableApiUsage") // omit warning about `Property`+`MapProperty`
      @Override
      public void execute(Test test) {
        // Add the StartTask's properties as "inputs" to the Test task, so the Test task is
        // executed, when those properties change.
        test.getInputs().properties(extension.getEnvironment().get());
        test.getInputs().properties(extension.getSystemProperties().get());
        test.getInputs().property("nessie.quarkus.arguments", extension.getArguments().get().toString());
        test.getInputs().property("nessie.quarkus.jvmArguments", extension.getJvmArguments().get().toString());
        RegularFile execJar = extension.getExecutableJar().getOrNull();
        if (execJar != null) {
          test.getInputs().property("nessie.quarkus.execJar", execJar);
        }
        test.getInputs().property("nessie.quarkus.javaVersion", extension.getJavaVersion().get());

        test.getInputs().files(appConfig);

        ProcessState processState = new ProcessState();

        // Start the Nessie-Quarkus-App only when the Test task actually runs
        test.doFirst(new Action<Task>() {
          @Override
          public void execute(Task ignore) {
            processState.quarkusStart(test);
          }
        });
        test.doLast(new Action<Task>() {
          @Override
          public void execute(Task task) {
            processState.quarkusStop(test);
          }
        });
      }
    });
  }
}
