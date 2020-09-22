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
package com.dremio.nessie.quarkus.maven;

import java.lang.reflect.Method;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugin.descriptor.PluginDescriptor;
import org.apache.maven.plugins.annotations.Component;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.plugins.annotations.ResolutionScope;
import org.eclipse.aether.RepositorySystem;
import org.eclipse.aether.RepositorySystemSession;
import org.eclipse.aether.repository.RemoteRepository;

import io.quarkus.bootstrap.app.CuratedApplication;
import io.quarkus.bootstrap.app.QuarkusBootstrap;
import io.quarkus.bootstrap.app.QuarkusBootstrap.Mode;
import io.quarkus.bootstrap.app.RunningQuarkusApplication;
import io.quarkus.bootstrap.app.StartupAction;
import io.quarkus.bootstrap.model.AppArtifact;
import io.quarkus.bootstrap.model.AppArtifactCoords;
import io.quarkus.bootstrap.model.AppModel;
import io.quarkus.bootstrap.resolver.BootstrapAppModelResolver;
import io.quarkus.bootstrap.resolver.maven.MavenArtifactResolver;

/**
 * Starting Quarkus application.
 */
@Mojo(name = "start", requiresDependencyResolution = ResolutionScope.NONE)
public class QuarkusAppStartMojo extends AbstractQuarkusAppMojo {
  /**
   * The entry point to Aether, i.e. the component doing all the work.
   *
   * @component
   */
  @Component
  private RepositorySystem repoSystem;

  /**
   * The current repository/network configuration of Maven.
   */
  @Parameter(defaultValue = "${repositorySystemSession}", readonly = true)
  private RepositorySystemSession repoSession;

  /**
   * The project's remote repositories to use for the resolution of artifacts and their dependencies.
   */
  @Parameter(defaultValue = "${project.remoteProjectRepositories}", readonly = true, required = true)
  private List<RemoteRepository> repos;

  /**
   * The plugin descriptor.
   */
  @Parameter(defaultValue = "${plugin}", readonly = true)
  private PluginDescriptor pluginDescriptor;

  /**
   * The application artifact id.
   *
   *<p>Needs to be present as a plugin dependency.
   *
   *<p>Supported format is groupId:artifactId[:type[:classifier]]:version
   */
  @Parameter(property = "nessie.apprunner.appArtifactId", required = true)
  private String appArtifactId;

  @Override
  public void execute() throws MojoExecutionException, MojoFailureException {
    if (isSkipped()) {
      getLog().debug("Execution is skipped");
      return;
    }

    final AppArtifactCoords appCoords = AppArtifactCoords.fromString(appArtifactId);
    final AppArtifact appArtifact = new AppArtifact(appCoords.getGroupId(), appCoords.getArtifactId(), appCoords.getClassifier(),
        appCoords.getType(), appCoords.getVersion());

    // Check that the artifact is present as it might cause some classloader confusion if not
    boolean appArtifactPresent = pluginDescriptor.getArtifacts().stream()
        .map(artifact -> new AppArtifactCoords(artifact.getGroupId(), artifact.getArtifactId(),
            artifact.getClassifier(), artifact.getType(), artifact.getVersion()))
        .filter(coords -> coords.equals(appCoords))
        .findAny()
        .isPresent();

    if (!appArtifactPresent) {
      throw new MojoExecutionException(String.format("Artifact %s not found in plugin dependencies", appCoords));
    }

    final AppModel appModel;
    try {
      MavenArtifactResolver resolver = MavenArtifactResolver.builder()
          .setWorkspaceDiscovery(false)
          .setRepositorySystem(repoSystem)
          .setRepositorySystemSession(repoSession)
          .setRemoteRepositories(repos)
          .build();

      appModel = new BootstrapAppModelResolver(resolver)
          .resolveModel(appArtifact);
    } catch (Exception e) {
      throw new MojoExecutionException("Failed to resolve application model " + appArtifact + " dependencies", e);
    }

    getLog().info("Starting Quarkus application.");

    final QuarkusBootstrap bootstrap = QuarkusBootstrap.builder()
        .setAppArtifact(appModel.getAppArtifact())
        .setBaseClassLoader(this.getClass().getClassLoader()) // use plugin classloader
        .setExistingModel(appModel)
        .setProjectRoot(getProject().getBasedir().toPath())
        .setTargetDirectory(Paths.get(getProject().getBuild().getDirectory()))
        .setIsolateDeployment(true)
        .setMode(Mode.TEST)
        .build();

    try {
      final CuratedApplication app = bootstrap.bootstrap();
      StartupAction startupAction = app.createAugmentor().createInitialRuntimeApplication();
      exitHandler(startupAction);
      RunningQuarkusApplication runningApp = startupAction.runMainClass();
      getLog().info("Quarkus application started.");
      setApplicationHandle(runningApp);
    } catch (Exception e) {
      throw new MojoExecutionException("Failure starting Nessie Daemon", e instanceof MojoExecutionException ? e.getCause() : e);
    }
  }

  private void exitHandler(StartupAction startupAction) throws ReflectiveOperationException {
    Consumer<Integer> consumer = i -> { };
    Method exitHandler = Arrays.stream(startupAction.getClassLoader().loadClass("io.quarkus.runtime.ApplicationLifecycleManager")
                                                .getMethods())
                               .filter(x -> x.getName().equals("setDefaultExitCodeHandler"))
                               .findFirst()
                               .orElseThrow(NoSuchMethodException::new);
    exitHandler.invoke(null, consumer);
  }
}
