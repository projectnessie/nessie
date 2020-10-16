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
import java.util.function.BiConsumer;

import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.project.MavenProject;
import org.eclipse.aether.RepositorySystem;
import org.eclipse.aether.RepositorySystemSession;

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
 * Test.
 *
 */
public class QuarkusApp implements AutoCloseable {

  private final RunningQuarkusApplication runningApp;

  private QuarkusApp(RunningQuarkusApplication runningApp) {
    this.runningApp = runningApp;
  }

  /**
   * Instantiate and start a quarkus application.
   *
   * <p>Instantiates and start a quarkus application using Quarkus bootstrap framework. Only one application can be started at a time
   * in the same classloader.
   *
   * @param project the Maven project
   * @param repoSystem the Maven repository system instanxce
   * @param repoSession the Maven repository system session
   * @param appArtifactId the quarkus application artifact id
   * @return a quarkus app instance
   * @throws MojoExecutionException if an error occurs during execution
   */
  public static QuarkusApp newApplication(MavenProject project, RepositorySystem repoSystem,
      RepositorySystemSession repoSession, String appArtifactId)
      throws MojoExecutionException {
    final AppArtifactCoords appCoords = AppArtifactCoords.fromString(appArtifactId);
    final AppArtifact appArtifact = new AppArtifact(appCoords.getGroupId(),
        appCoords.getArtifactId(), appCoords.getClassifier(), appCoords.getType(),
        appCoords.getVersion());

    final AppModel appModel;
    try {
      MavenArtifactResolver resolver = MavenArtifactResolver.builder().setWorkspaceDiscovery(false)
          .setRepositorySystem(repoSystem).setRepositorySystemSession(repoSession)
          .setRemoteRepositories(project.getRemoteProjectRepositories()).build();

      appModel = new BootstrapAppModelResolver(resolver).setDevMode(false).setTest(false)
          .resolveModel(appArtifact);
    } catch (Exception e) {
      throw new MojoExecutionException(
          "Failed to resolve application model " + appArtifact + " dependencies", e);
    }

    final QuarkusBootstrap bootstrap = QuarkusBootstrap.builder()
        .setAppArtifact(appModel.getAppArtifact())
        .setBaseClassLoader(QuarkusApp.class.getClassLoader()).setExistingModel(appModel)
        .setProjectRoot(project.getBasedir().toPath())
        .setTargetDirectory(Paths.get(project.getBuild().getDirectory())).setIsolateDeployment(true)
        .setMode(Mode.TEST).build();

    try {
      final CuratedApplication app = bootstrap.bootstrap();
      StartupAction startupAction = app.createAugmentor().createInitialRuntimeApplication();
      exitHandler(startupAction);
      RunningQuarkusApplication runningApp = startupAction.runMainClass();
      return new QuarkusApp(runningApp);
    } catch (Exception e) {
      throw new MojoExecutionException("Failure starting Nessie Daemon",
          e instanceof MojoExecutionException ? e.getCause() : e);
    }
  }

  private static void exitHandler(StartupAction startupAction) throws ReflectiveOperationException {
    final BiConsumer<Integer, Throwable> consumer = (i, t) -> {};
    final Class<?> applicationLifecyceManagerClass = Class.forName(
        "io.quarkus.runtime.ApplicationLifecycleManager", true, startupAction.getClassLoader());
    final Method exitHandler = applicationLifecyceManagerClass.getMethod("setDefaultExitCodeHandler", BiConsumer.class);
    exitHandler.invoke(null, consumer);
  }

  @Override public void close() throws Exception {
    runningApp.close();
  }
}
