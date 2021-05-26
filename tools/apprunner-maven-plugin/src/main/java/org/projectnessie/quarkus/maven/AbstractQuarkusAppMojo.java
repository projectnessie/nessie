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

import io.quarkus.bootstrap.model.AppArtifact;
import io.quarkus.bootstrap.model.AppArtifactCoords;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.project.MavenProject;

/*
 * Base class to share configuration between mojo.
 */
abstract class AbstractQuarkusAppMojo extends AbstractMojo {
  private static final String CONTEXT_KEY = "nessie.quarkus.app";

  /** Maven project. */
  @Parameter(defaultValue = "${project}", readonly = true, required = true)
  private MavenProject project;

  /** Whether execution should be skipped. */
  @Parameter(property = "nessie.apprunner.skip", required = false, defaultValue = "false")
  private boolean skip;

  /** Execution id for the app. */
  @Parameter(property = "nessie.apprunner.executionId", required = false, defaultValue = "default")
  private String executionId;

  public boolean isSkipped() {
    return skip;
  }

  public String getExecutionId() {
    return executionId;
  }

  public MavenProject getProject() {
    return project;
  }

  private String getContextKey() {
    final String key = CONTEXT_KEY + '.' + getExecutionId();
    return key;
  }

  protected AutoCloseable getApplication() {
    final String key = getContextKey();
    return (AutoCloseable) project.getContextValue(key);
  }

  protected void resetApplication() {
    final String key = getContextKey();
    project.setContextValue(key, null);
  }

  protected void setApplicationHandle(AutoCloseable application) {
    final String key = getContextKey();
    final Object previous = project.getContextValue(key);
    if (previous != null) {
      getLog()
          .warn(
              String.format("Found a previous application for execution id %s.", getExecutionId()));
    }
    project.setContextValue(key, application);
  }

  static AppArtifact fromString(String artifactId) {
    AppArtifactCoords coords = AppArtifactCoords.fromString(artifactId);
    return new AppArtifact(
        coords.getGroupId(),
        coords.getArtifactId(),
        coords.getClassifier(),
        coords.getType(),
        coords.getVersion());
  }
}
