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

import java.io.File;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

import org.gradle.api.Project;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.artifacts.Dependency;
import org.gradle.api.artifacts.ModuleVersionIdentifier;
import org.gradle.api.artifacts.ResolvedArtifact;
import org.gradle.api.internal.artifacts.dependencies.DefaultExternalModuleDependency;
import org.gradle.api.internal.artifacts.ivyservice.DefaultUnresolvedDependency;
import org.gradle.tooling.GradleConnector;
import org.gradle.tooling.ModelBuilder;
import org.gradle.tooling.ProjectConnection;

import io.quarkus.bootstrap.app.CuratedApplication;
import io.quarkus.bootstrap.app.QuarkusBootstrap;
import io.quarkus.bootstrap.app.QuarkusBootstrap.Mode;
import io.quarkus.bootstrap.app.RunningQuarkusApplication;
import io.quarkus.bootstrap.app.StartupAction;
import io.quarkus.bootstrap.model.AppArtifact;
import io.quarkus.bootstrap.model.AppDependency;
import io.quarkus.bootstrap.model.AppModel;
import io.quarkus.bootstrap.resolver.AppModelResolverException;
import io.quarkus.bootstrap.resolver.QuarkusGradleModelFactory;
import io.quarkus.bootstrap.resolver.model.QuarkusModel;
import io.quarkus.bootstrap.util.QuarkusModelHelper;

import com.google.common.collect.ImmutableList;


/**
 * Test.
 */
public class QuarkusApp implements AutoCloseable {

  private final RunningQuarkusApplication runningApp;

  private QuarkusApp(RunningQuarkusApplication runningApp) {
    this.runningApp = runningApp;
  }

  public static QuarkusApp newApplication(Configuration configuration, Configuration deploy, Project project) {

    final AppModel appModel;
//    try {
//      AppArtifact appArtifact = toDependency(configuration.getDependencies().stream().findFirst().get());
//      Optional<String> path = configuration.getFiles().stream().map(File::getAbsolutePath).filter(x->x.contains(appArtifact.getArtifactId()))
//        .filter(x->x.contains(appArtifact.getGroupId().replace(".", File.separator)))
//        .filter(x->x.contains(appArtifact.getVersion()))
//        .findFirst();
//      appArtifact.setPath(Paths.get(path.orElseThrow(() -> new UnsupportedOperationException("xxx"))));
//      QuarkusModel qm = QuarkusGradleModelFactory.create(project.getProjectDir(), "test");
//      appModel = QuarkusModelHelper.convert(qm, appArtifact);
//    } catch (Exception e) {
//      throw new RuntimeException(e);
//    }

    appModel = convert(configuration, deploy);

    URL[] urls = appModel.getFullDeploymentDeps().stream().map(QuarkusApp::toUrl).toArray(URL[]::new);
    ClassLoader cl = new URLClassLoader(urls, QuarkusApp.class.getClassLoader());
    final QuarkusBootstrap bootstrap = QuarkusBootstrap.builder()
      .setAppArtifact(appModel.getAppArtifact())
      .setBaseClassLoader(QuarkusApp.class.getClassLoader()).setExistingModel(appModel)
      .setProjectRoot(project.getProjectDir().toPath())
      .setTargetDirectory(Paths.get(project.getBuildDir().getPath())).setIsolateDeployment(true)
      .setMode(Mode.TEST).build();

    try {
      final CuratedApplication app = bootstrap.bootstrap();
      StartupAction startupAction = app.createAugmentor().createInitialRuntimeApplication();
      exitHandler(startupAction);
      RunningQuarkusApplication runningApp = startupAction.runMainClass();
      return new QuarkusApp(runningApp);
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }
  }

  private static URL toUrl(AppDependency dep) {
    try {
      return dep.getArtifact().getPaths().getSinglePath().toUri().toURL();
    } catch (MalformedURLException e) {
      throw new RuntimeException(e);
    }
  }
  private static void exitHandler(StartupAction startupAction) throws ReflectiveOperationException {
    Consumer<Integer> consumer = i -> {
    };
    Method exitHandler = Arrays
      .stream(startupAction.getClassLoader()
        .loadClass("io.quarkus.runtime.ApplicationLifecycleManager").getMethods())
      .filter(x -> x.getName().equals("setDefaultExitCodeHandler")).findFirst()
      .orElseThrow(NoSuchMethodException::new);
    exitHandler.invoke(null, consumer);
  }

  @Override
  public void close() throws Exception {
    runningApp.close();
  }

  public static AppModel convert(Configuration configuration, Configuration deploy) {

    AppModel.Builder appBuilder = new AppModel.Builder();

    final Set<AppDependency> userDeps = new HashSet<>();
    final Set<AppDependency> deployDeps = new HashSet<>();
    configuration.getResolvedConfiguration()
      .getResolvedArtifacts()
      .stream()
      .map(QuarkusApp::toDependency).forEach(userDeps::add);
    configuration.getResolvedConfiguration()
      .getResolvedArtifacts()
      .stream()
      .filter(x -> x.getName().contains("quarkus"))
      .map(x -> new DefaultExternalModuleDependency(x.getModuleVersion().getId().getGroup(), x.getName() + ((x.getName().contains("deployment")) ? "" : "-deployment"), x.getModuleVersion().getId().getVersion()))
      .forEach(x -> deploy.getDependencies().add(x));

    Configuration newDeploy = deploy.copy();
    newDeploy.getResolvedConfiguration().getLenientConfiguration().getUnresolvedModuleDependencies().forEach(x -> deploy.getDependencies().remove(unresolvedToExternal((DefaultUnresolvedDependency) x)));
    deploy.getResolvedConfiguration().getResolvedArtifacts().stream()
      .map(QuarkusApp::toDependency).forEach(deployDeps::add);
    AppArtifact appArtifact = toDependency(configuration.getDependencies().stream().findFirst().get());
    Optional<String> path = configuration.getFiles().stream().map(File::getAbsolutePath).filter(x->x.contains(appArtifact.getArtifactId()))
      .filter(x->x.contains(appArtifact.getGroupId().replace(".", File.separator)))
      .filter(x->x.contains(appArtifact.getVersion()))
      .findFirst();
    appArtifact.setPath(Paths.get(path.orElseThrow(() -> new UnsupportedOperationException("xxx"))));
    //deployDeps.removeAll(userDeps);
    List<AppDependency> userDeps2 = new ArrayList<>(userDeps);
    userDeps.addAll(deployDeps);
    List<AppDependency> allDeps = new ArrayList<>(userDeps);
    appBuilder.addRuntimeDeps(new ArrayList<>(userDeps2))
      .addFullDeploymentDeps(allDeps)
      .addDeploymentDeps(new ArrayList<>(deployDeps))
      .setAppArtifact(appArtifact);
    return appBuilder.build();
  }

  private static Dependency unresolvedToExternal(DefaultUnresolvedDependency dependency) {
    return new DefaultExternalModuleDependency(dependency.getSelector().getGroup(), dependency.getSelector().getName(), dependency.getSelector().getVersion());
  }
  private static AppArtifact toDependency(Dependency dependency) {
    AppArtifact artifact = new AppArtifact(dependency.getGroup(), dependency.getName(), null,
      "jar", dependency.getVersion());
//    artifact.setPaths(QuarkusModelHelper.toPathsCollection(ImmutableList.of(dependency.getFile())));
    return artifact;
  }

  private static AppDependency toDependency(ResolvedArtifact dependency) {
    ModuleVersionIdentifier id = dependency.getModuleVersion().getId();
    AppArtifact artifact = new AppArtifact(id.getGroup(), dependency.getName(), dependency.getClassifier(),
      dependency.getType(), id.getVersion());
    artifact.setPaths(QuarkusModelHelper.toPathsCollection(ImmutableList.of(dependency.getFile())));
    return new AppDependency(artifact, "runtime");
  }
}
