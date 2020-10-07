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
package com.dremio.nessie.quarkus.gradle;


import org.gradle.api.Action;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.artifacts.DependencySet;


public class QuarkusAppPlugin implements Plugin<Project> {

  private String foo = "Bar";


  @Override
  public void apply(Project target) {
//    QuarkusAppExtension extension = target.getExtensions().create("quarkusAppRunner", QuarkusAppExtension.class, target);

    final Configuration config = target.getConfigurations().create("quarkusAppRunnerConfig")
      .setVisible(false)
      .setDescription("The config for the Quarkus Runner.");

    config.defaultDependencies(new Action<DependencySet>() {
      public void execute(DependencySet dependencies) {
        dependencies.add(target.getDependencies().create("org.projectnessie:nessie-quarkus:0.1-SNAPSHOT"));
      }
    });

    final Configuration configDeploy = target.getConfigurations().create("quarkusAppRunnerDeploy")
      .setVisible(false)
      .setDescription("The config for the Quarkus Runner.");

    configDeploy.defaultDependencies(new Action<DependencySet>() {
      public void execute(DependencySet dependencies) {
        dependencies.add(target.getDependencies().create("io.quarkus:quarkus-bootstrap-core:1.8.1.Final"));
      }
    });

    target.getTasks().register("quarkus-start", StartTask.class, new Action<StartTask>() {
      @Override
      public void execute(StartTask task) {
        task.setConfig(config, configDeploy);
      }
    });
    target.getTasks().register("quarkus-stop", new Action<Task>() {
      @Override
      public void execute(Task task) {

      }
    });

  }
}
