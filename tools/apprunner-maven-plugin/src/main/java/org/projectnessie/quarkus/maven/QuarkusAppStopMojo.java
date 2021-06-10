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
package org.projectnessie.quarkus.maven;

import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.ResolutionScope;

/** Stop Quarkus application. */
@Mojo(name = "stop", requiresDependencyResolution = ResolutionScope.NONE, threadSafe = true)
public class QuarkusAppStopMojo extends AbstractQuarkusAppMojo {
  /** Mojo execution. */
  @Override
  public void execute() throws MojoExecutionException, MojoFailureException {
    if (isSkipped()) {
      getLog().info("Stopping Quarkus application.");
      return;
    }

    final AutoCloseable application = getApplication();
    if (application == null) {
      getLog().warn(String.format("No application found for execution id '%s'.", getExecutionId()));
    }

    try {
      application.close();
      getLog().info("Quarkus application stopped.");
    } catch (Exception e) {
      throw new MojoExecutionException("Error while stopping Quarkus application", e);
    } finally {
      resetApplication();
    }
  }
}
