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

import static io.quarkus.bootstrap.resolver.maven.DeploymentInjectingDependencyVisitor.toArtifact;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.eclipse.aether.artifact.Artifact;
import org.gradle.api.GradleException;
import org.gradle.api.Project;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.artifacts.Dependency;
import org.gradle.api.artifacts.ModuleVersionIdentifier;
import org.gradle.api.artifacts.ResolvedArtifact;
import org.gradle.api.internal.artifacts.dependencies.DefaultExternalModuleDependency;

import com.google.common.collect.ImmutableList;

import io.quarkus.bootstrap.BootstrapConstants;
import io.quarkus.bootstrap.app.CuratedApplication;
import io.quarkus.bootstrap.app.QuarkusBootstrap;
import io.quarkus.bootstrap.app.QuarkusBootstrap.Mode;
import io.quarkus.bootstrap.app.RunningQuarkusApplication;
import io.quarkus.bootstrap.app.StartupAction;
import io.quarkus.bootstrap.model.AppArtifact;
import io.quarkus.bootstrap.model.AppDependency;
import io.quarkus.bootstrap.model.AppModel;
import io.quarkus.bootstrap.util.QuarkusModelHelper;
import io.quarkus.bootstrap.util.ZipUtils;


/**
 * Start and Stop quarkus.
 */
public class QuarkusApp implements AutoCloseable {
  private final RunningQuarkusApplication runningApp;

  private QuarkusApp(RunningQuarkusApplication runningApp) {
    this.runningApp = runningApp;
  }

  public static QuarkusApp newApplication(Configuration configuration, Project project) {

    Configuration deploy = project.getConfigurations().create("quarkusAppDeploy");
    final AppModel appModel;

    appModel = convert(configuration, deploy);

    URL[] urls = appModel.getFullDeploymentDeps().stream().map(QuarkusApp::toUrl).toArray(URL[]::new);
    ClassLoader cl = new URLClassLoader(urls, QuarkusApp.class.getClassLoader());
    final QuarkusBootstrap bootstrap = QuarkusBootstrap.builder()
      .setAppArtifact(appModel.getAppArtifact())
      .setBaseClassLoader(cl).setExistingModel(appModel)
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
      throw new GradleException("Unable to start Quarkus", e);
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
    // set of dependencies requested by the user (usually the artifact that contains the quarkus app)
    Set<AppArtifact> baseConfigs = configuration.getDependencies()
      .stream()
      .map(QuarkusApp::toDependency)
      .collect(Collectors.toSet());
    assert baseConfigs.size() == 1; // currently we only know how to support the single quarkus app artifact
    AppArtifact appArtifact = baseConfigs.iterator().next();
    // resolve all dependencies of the artifacts from above.
    configuration.getResolvedConfiguration()
      .getResolvedArtifacts()
      .stream()
      .map(QuarkusApp::toDependency)
      .filter(x -> !appArtifact.equals(x.getArtifact())) // remove base deps, accounted for below
      .forEach(userDeps::add);

    // for each user dependency check if it has any associated deployment deps and add those to the deploy config
    userDeps.stream()
      .map(x -> QuarkusApp.handleMetaInf(appBuilder, x))
      .filter(Objects::nonNull)
      .map(x -> new DefaultExternalModuleDependency(x.getGroupId(), x.getArtifactId(), x.getVersion()))
      .forEach(x -> deploy.getDependencies().add(x));

    // resolve the deployment deps and their dependencies
    deploy.getResolvedConfiguration().getResolvedArtifacts().stream()
      .map(QuarkusApp::toDependency).forEach(deployDeps::add);


    // find the path of the base app artifact
    Optional<String> path = configuration.getFiles().stream().map(File::getAbsolutePath)
      .filter(x->x.contains(appArtifact.getArtifactId()))
      .filter(x->x.contains(appArtifact.getGroupId().replace(".", File.separator)))
      .filter(x->x.contains(appArtifact.getVersion()))
      .findFirst();
    appArtifact.setPath(Paths.get(path.orElseThrow(() -> new UnsupportedOperationException("xxx"))));

    // combine user and deploy deps and build app model
    List<AppDependency> allDeps = new ArrayList<>(userDeps);
    allDeps.addAll(deployDeps);
    appBuilder.addRuntimeDeps(new ArrayList<>(userDeps))
      .addFullDeploymentDeps(allDeps)
      .addDeploymentDeps(new ArrayList<>(deployDeps))
      .setAppArtifact(appArtifact);
    return appBuilder.build();
  }

  /**
   * for each dependent artifact read its META-INF looking for quarkus metadata
   */
  private static AppArtifact handleMetaInf(AppModel.Builder appBuilder, AppDependency dependency) {
    try {
      Path path = dependency.getArtifact().getPaths().getSinglePath();
      try (FileSystem artifactFs = ZipUtils.newFileSystem(path)) {
        Path metaInfPath = artifactFs.getPath(BootstrapConstants.META_INF);
        final Path p = metaInfPath.resolve(BootstrapConstants.DESCRIPTOR_FILE_NAME);
        return Files.exists(p) ? processPlatformArtifact(appBuilder, dependency.getArtifact(), p) : null;
      }
    } catch (IOException e) {
      throw new GradleException("couldn't read artifact", e);
    }
  }

  /**
   * Search for quarkus metadata and if found augment the AppModel builder. Return any deployment deps.
   */
  private static AppArtifact processPlatformArtifact(AppModel.Builder appBuilder, AppArtifact node, Path descriptor) throws IOException {
    final Properties rtProps = resolveDescriptor(descriptor);
    if (rtProps == null) {
      return null;
    }
    appBuilder.handleExtensionProperties(rtProps, node.toString());
    final String value = rtProps.getProperty(BootstrapConstants.PROP_DEPLOYMENT_ARTIFACT);
    if (value == null) {
      return null;
    }
    Artifact deploymentArtifact = toArtifact(value);
    if (deploymentArtifact.getVersion() == null || deploymentArtifact.getVersion().isEmpty()) {
      deploymentArtifact = deploymentArtifact.setVersion(node.getVersion());
    }

    return new AppArtifact(deploymentArtifact.getGroupId(), deploymentArtifact.getArtifactId(), deploymentArtifact.getClassifier(), "jar", deploymentArtifact.getVersion());
  }

  private static Properties resolveDescriptor(final Path path) throws IOException {
    final Properties rtProps;
    if (!Files.exists(path)) {
      // not a platform artifact
      return null;
    }
    rtProps = new Properties();
    try (BufferedReader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
      rtProps.load(reader);
    }
    return rtProps;
  }

  private static AppArtifact toDependency(Dependency dependency) {
    AppArtifact artifact = new AppArtifact(dependency.getGroup(), dependency.getName(), null,
      "jar", dependency.getVersion());
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
