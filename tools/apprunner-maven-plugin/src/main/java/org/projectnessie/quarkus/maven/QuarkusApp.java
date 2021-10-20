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

import io.quarkus.bootstrap.app.AdditionalDependency;
import io.quarkus.bootstrap.app.CuratedApplication;
import io.quarkus.bootstrap.app.QuarkusBootstrap;
import io.quarkus.bootstrap.app.QuarkusBootstrap.Mode;
import io.quarkus.bootstrap.app.RunningQuarkusApplication;
import io.quarkus.bootstrap.app.StartupAction;
import io.quarkus.bootstrap.model.AppArtifactCoords;
import io.quarkus.bootstrap.model.ApplicationModel;
import io.quarkus.bootstrap.resolver.BootstrapAppModelResolver;
import io.quarkus.bootstrap.resolver.maven.MavenArtifactResolver;
import java.lang.reflect.Method;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.function.BiConsumer;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.project.MavenProject;
import org.eclipse.aether.RepositorySystem;
import org.eclipse.aether.RepositorySystemSession;

/** Test. */
public class QuarkusApp implements AutoCloseable {

  private static final String MOJO_CONFIG_SOURCE_CLASSNAME =
      "org.projectnessie.quarkus.maven.MojoConfigSource";
  private final RunningQuarkusApplication runningApp;

  protected QuarkusApp(RunningQuarkusApplication runningApp) {
    this.runningApp = runningApp;
  }

  /**
   * Instantiate and start a quarkus application.
   *
   * <p>Instantiates and start a quarkus application using Quarkus bootstrap framework. Only one
   * application can be started at a time in the same classloader.
   *
   * @param project the Maven project
   * @param repoSystem the Maven repository system instanxce
   * @param repoSession the Maven repository system session
   * @param appArtifactId the quarkus application artifact id
   * @param applicationProperties the extra application properties
   * @return a quarkus app instance
   * @throws MojoExecutionException if an error occurs during execution
   */
  public static QuarkusApp newApplication(
      MavenProject project,
      RepositorySystem repoSystem,
      RepositorySystemSession repoSession,
      String appArtifactId,
      Properties applicationProperties)
      throws MojoExecutionException {
    AppArtifactCoords appCoords = AppArtifactCoords.fromString(appArtifactId);

    ApplicationModel appModel;
    try {
      MavenArtifactResolver resolver =
          MavenArtifactResolver.builder()
              .setWorkspaceDiscovery(false)
              .setRepositorySystem(repoSystem)
              .setRepositorySystemSession(repoSession)
              .setRemoteRepositories(project.getRemoteProjectRepositories())
              .build();

      appModel =
          new BootstrapAppModelResolver(resolver)
              .setDevMode(false)
              .setTest(false)
              .resolveModel(appCoords);
    } catch (Exception e) {
      throw new MojoExecutionException(
          "Failed to resolve application model " + appCoords + " dependencies", e);
    }

    return newApplication(
        appModel,
        project.getBasedir().toPath(),
        Paths.get(project.getBuild().getDirectory()),
        applicationProperties);
  }

  /**
   * Instantiate and start a quarkus application.
   *
   * <p>Instantiates and start a quarkus application using Quarkus bootstrap framework. Only one
   * application can be started at a time in the same classloader.
   *
   * @param appModel the application model
   * @param projectRoot the current project directory
   * @param targetDirectory the target directory
   * @param applicationProperties the extra application properties
   * @return a quarkus app instance
   * @throws MojoExecutionException if an error occurs during execution
   */
  public static QuarkusApp newApplication(
      ApplicationModel appModel,
      Path projectRoot,
      Path targetDirectory,
      Properties applicationProperties)
      throws MojoExecutionException {
    return newApplication(
        appModel,
        projectRoot,
        targetDirectory,
        applicationProperties,
        QuarkusApp.class.getClassLoader());
  }

  /**
   * Instantiate and start a quarkus application.
   *
   * <p>Instantiates and start a quarkus application using Quarkus bootstrap framework. Only one
   * application can be started at a time in the same classloader.
   *
   * @param appModel the application model
   * @param projectRoot the current project directory
   * @param targetDirectory the target directory
   * @param applicationProperties the extra application properties
   * @param classLoader the classloader to use when starting the application
   * @return a quarkus app instance
   * @throws MojoExecutionException if an error occurs during execution
   */
  public static QuarkusApp newApplication(
      ApplicationModel appModel,
      Path projectRoot,
      Path targetDirectory,
      Properties applicationProperties,
      ClassLoader classLoader)
      throws MojoExecutionException {
    final AdditionalDependency mojoConfigSourceDependency = findMojoConfigSourceDependency();

    final QuarkusBootstrap bootstrap =
        QuarkusBootstrap.builder()
            .setAppArtifact(appModel.getAppArtifact())
            .setBaseClassLoader(classLoader)
            .setExistingModel(appModel)
            .setProjectRoot(projectRoot)
            .setTargetDirectory(targetDirectory)
            .setIsolateDeployment(true)
            .setMode(Mode.TEST)
            .addAdditionalApplicationArchive(mojoConfigSourceDependency)
            .build();

    try {
      final CuratedApplication app = bootstrap.bootstrap();
      StartupAction startupAction = app.createAugmentor().createInitialRuntimeApplication();
      configureMojConfigSource(startupAction, applicationProperties);
      exitHandler(startupAction);
      RunningQuarkusApplication runningApp = startupAction.runMainClass();
      return new QuarkusApp(runningApp);
    } catch (Exception e) {
      throw new MojoExecutionException(
          "Failure starting Nessie Daemon", e instanceof MojoExecutionException ? e.getCause() : e);
    }
  }

  private static void configureMojConfigSource(
      StartupAction startupAction, Properties applicationProperties)
      throws ReflectiveOperationException {
    if (applicationProperties == null) {
      return;
    }

    final Class<?> mojoConfigSourceClass =
        startupAction.getClassLoader().loadClass(MOJO_CONFIG_SOURCE_CLASSNAME);
    final Method method =
        mojoConfigSourceClass.getDeclaredMethod("setProperties", Properties.class);
    method.invoke(null, applicationProperties);
  }

  private static void exitHandler(StartupAction startupAction) throws ReflectiveOperationException {
    final BiConsumer<Integer, Throwable> consumer = (i, t) -> {};
    final Class<?> applicationLifecyceManagerClass =
        Class.forName(
            "io.quarkus.runtime.ApplicationLifecycleManager", true, startupAction.getClassLoader());
    final Method exitHandler =
        applicationLifecyceManagerClass.getMethod("setDefaultExitCodeHandler", BiConsumer.class);
    exitHandler.invoke(null, consumer);
  }

  private static AdditionalDependency findMojoConfigSourceDependency()
      throws MojoExecutionException {
    try {
      // We do need to initialize the class
      final Class<?> mojoConfigSourceClass =
          QuarkusApp.class.getClassLoader().loadClass(MOJO_CONFIG_SOURCE_CLASSNAME);
      final URL mojoConfigSourceClasslocation =
          mojoConfigSourceClass.getProtectionDomain().getCodeSource().getLocation();
      return new AdditionalDependency(
          Paths.get(mojoConfigSourceClasslocation.toURI()), false, false);
    } catch (ClassNotFoundException | URISyntaxException e) {
      throw new MojoExecutionException(e.getMessage(), e);
    }
  }

  @Override
  public void close() throws Exception {
    runningApp.close();
  }
}
