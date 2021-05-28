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
import org.gradle.api.tasks.testing.Test;

@SuppressWarnings("Convert2Lambda") // Gradle complains when using lambdas (build-cache won't wonk)
public class QuarkusAppPlugin implements Plugin<Project> {

  static final String START_TASK_NAME = "nessie-quarkus-start";
  static final String STOP_TASK_NAME = "nessie-quarkus-stop";
  static final String EXTENSION_NAME = "nessieQuarkusApp";

  /** The configuration that contains the Quarkus server application as the only dependency. */
  static final String APP_CONFIG_NAME = "nessieQuarkusServer";
  /** The configuration that contains optional runtime dependencies, if required. */
  static final String RUNTIME_CONFIG_NAME = "nessieQuarkusRuntime";
  /**
   * INTERNAL configuration that extends {@link #APP_CONFIG_NAME} + {@link #RUNTIME_CONFIG_NAME}.
   */
  static final String LAUNCH_CONFIG_NAME = "nessieQuarkusLaunch";

  @Override
  public void apply(Project target) {
    QuarkusAppExtension extension =
        target.getExtensions().create(EXTENSION_NAME, QuarkusAppExtension.class, target);

    Configuration appConfig =
        target
            .getConfigurations()
            .create(APP_CONFIG_NAME)
            .setDescription(
                "References the Nessie-Quarkus server dependency, only a single dependency allowed.");
    Configuration runtimeConfig =
        target
            .getConfigurations()
            .create(RUNTIME_CONFIG_NAME)
            .setDescription("Optional/additional runtime dependencies.");
    Configuration launchConfig =
        target
            .getConfigurations()
            .create(LAUNCH_CONFIG_NAME)
            .setDescription(
                String.format(
                    "Extends %s and %s - plugin internal use only.",
                    APP_CONFIG_NAME, RUNTIME_CONFIG_NAME))
            .setVisible(false)
            .extendsFrom(runtimeConfig, appConfig);

    // Cannot use the task name "test" here, because the "test" task might not have been registered
    // yet.
    // This `withType(Test.class...)` construct will configure any current and future task of type
    // `Test`.
    target
        .getTasks()
        .withType(
            Test.class,
            new Action<Test>() {
              @SuppressWarnings("UnstableApiUsage") // omit warning about `Property`+`MapProperty`
              @Override
              public void execute(Test test) {
                test.dependsOn(START_TASK_NAME);
                test.finalizedBy(STOP_TASK_NAME);

                // Add the StartTask's properties as "inputs" to the Test task, so the Test task is
                // executed, when those properties change.
                test.getInputs().property("nessie.quarkus.props", extension.getPropsProperty());
                test.getInputs()
                    .property(
                        "quarkus.native.builderImage", extension.getNativeBuilderImageProperty());

                test.getInputs().files(launchConfig);

                // Start the Nessie-Quarkus-App only when the Test task actually runs
                test.doFirst(
                    new Action<Task>() {
                      @Override
                      public void execute(Task ignore) {
                        StartTask startTask =
                            (StartTask) target.getTasks().getByName(START_TASK_NAME);
                        startTask.quarkusStart(test);
                      }
                    });
              }
            });

    target.getTasks().register(START_TASK_NAME, StartTask.class);
    target.getTasks().register(STOP_TASK_NAME, StopTask.class);
  }
}
