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

import static io.quarkus.bootstrap.util.DependencyNodeUtils.toArtifact;
import static org.projectnessie.quarkus.gradle.QuarkusAppPlugin.APP_CONFIG_NAME;
import static org.projectnessie.quarkus.gradle.QuarkusAppPlugin.LAUNCH_CONFIG_NAME;
import static org.projectnessie.quarkus.gradle.QuarkusAppPlugin.RUNTIME_CONFIG_NAME;

import com.google.common.collect.ImmutableList;
import io.quarkus.bootstrap.BootstrapConstants;
import io.quarkus.bootstrap.app.RunningQuarkusApplication;
import io.quarkus.bootstrap.model.AppArtifact;
import io.quarkus.bootstrap.model.AppDependency;
import io.quarkus.bootstrap.model.AppModel;
import io.quarkus.bootstrap.util.PathsUtils;
import io.quarkus.bootstrap.util.ZipUtils;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.maven.plugin.MojoExecutionException;
import org.eclipse.aether.artifact.Artifact;
import org.gradle.api.GradleException;
import org.gradle.api.Project;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.artifacts.Dependency;
import org.gradle.api.artifacts.ModuleVersionIdentifier;
import org.gradle.api.artifacts.ResolvedArtifact;
import org.gradle.api.internal.artifacts.dependencies.DefaultExternalModuleDependency;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Start and Stop quarkus. */
public class QuarkusApp extends org.projectnessie.quarkus.maven.QuarkusApp {
  private static final Logger LOGGER = LoggerFactory.getLogger(QuarkusApp.class);

  protected QuarkusApp(RunningQuarkusApplication runningApp) {
    super(runningApp);
  }

  public static AutoCloseable newApplication(Project project, Properties props) {

    final AppModel appModel;

    appModel = convert(project, props);
    URL[] urls =
        appModel.getFullDeploymentDeps().stream().map(QuarkusApp::toUrl).toArray(URL[]::new);
    ClassLoader cl =
        new URLClassLoader(urls, org.projectnessie.quarkus.maven.QuarkusApp.class.getClassLoader());
    try {
      return org.projectnessie.quarkus.maven.QuarkusApp.newApplication(
          appModel,
          project.getProjectDir().toPath(),
          Paths.get(project.getBuildDir().getPath()),
          props,
          cl);
    } catch (MojoExecutionException e) {
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

  public static AppModel convert(Project project, Properties props) {

    Configuration deploy = project.getConfigurations().create("nessieQuarkusDeploy");

    AppModel.Builder appBuilder = new AppModel.Builder();

    Configuration appConfig = project.getConfigurations().getByName(APP_CONFIG_NAME);
    Configuration runtimeConfig = project.getConfigurations().getByName(RUNTIME_CONFIG_NAME);
    Configuration launchConfig = project.getConfigurations().getByName(LAUNCH_CONFIG_NAME);

    LOGGER.debug("Resolving dependencies of configuration '{}'", appConfig.getName());

    // Maintain the order of the dependencies
    Set<AppDependency> userDeps = new LinkedHashSet<>();
    Set<AppDependency> deployDeps = new LinkedHashSet<>();

    // The only application artifact - this is for example org.projectnessie:quarkus-server
    AppArtifact appArtifact = getAppArtifact(appConfig);

    LOGGER.info("Chose '{}' as the application artifact", appArtifact);
    LOGGER.info("Runtime '{}' dependencies:", RUNTIME_CONFIG_NAME);
    runtimeConfig.getDependencies().stream()
        .map(QuarkusApp::toDependency)
        .forEach(artifact -> LOGGER.info("  {}", artifact));

    // resolve all dependencies of the artifacts from above.
    launchConfig.getResolvedConfiguration().getResolvedArtifacts().stream()
        .map(QuarkusApp::toDependency)
        .filter(
            artifact ->
                !appArtifact.equals(
                    artifact.getArtifact())) // remove base deps, accounted for below
        .forEach(userDeps::add);

    LOGGER.debug("Configuration '{}' resolved dependencies:", launchConfig.getName());
    userDeps.forEach(artifact -> LOGGER.debug("{}", artifact));

    // for each user dependency check if it has any associated deployment deps and add those to the
    // deploy config
    userDeps.stream()
        .map(artifact -> QuarkusApp.handleMetaInf(appBuilder, artifact))
        .filter(Objects::nonNull)
        .map(
            artifact ->
                new DefaultExternalModuleDependency(
                    artifact.getGroupId(), artifact.getArtifactId(), artifact.getVersion()))
        .forEach(artifact -> deploy.getDependencies().add(artifact));

    // resolve the deployment deps and their dependencies
    deploy.getResolvedConfiguration().getResolvedArtifacts().stream()
        .map(QuarkusApp::toDependency)
        .forEach(deployDeps::add);

    LOGGER.debug("Resolving application artifact {}", appArtifact);

    // find the path of the base app artifact. We want the direct dependency only so we do a
    // difference between all
    // files and the the files we know from indirect dependencies. The leftovers are the files we
    // want.
    final Set<File> userDepFiles =
        userDeps.stream()
            .map(x -> x.getArtifact().getPaths().getSinglePath().toFile())
            .collect(Collectors.toSet());
    final Path path =
        appConfig.getFiles().stream()
            .filter(((Predicate<File>) userDepFiles::contains).negate())
            .map(File::toPath)
            .findFirst()
            .orElseThrow(
                () ->
                    new UnsupportedOperationException(
                        String.format("Unknown path for app artifact %s", appArtifact)));

    LOGGER.info("Resolved application artifact {} to {}", appArtifact, path);

    appArtifact.setPath(path);

    // combine user and deploy deps and build app model
    List<AppDependency> allDeps = new ArrayList<>(userDeps);
    allDeps.addAll(deployDeps);
    props.forEach((k, v) -> System.setProperty(k.toString(), v.toString()));
    appBuilder
        .addRuntimeDeps(new ArrayList<>(userDeps))
        .addFullDeploymentDeps(allDeps)
        .addDeploymentDeps(new ArrayList<>(deployDeps))
        .setAppArtifact(appArtifact);
    return appBuilder.build();
  }

  private static AppArtifact getAppArtifact(Configuration appConfig) {
    Set<AppArtifact> baseConfigs =
        appConfig.getDependencies().stream()
            .map(QuarkusApp::toDependency)
            .collect(Collectors.toSet());
    LOGGER.debug("Configuration '{}' dependencies:", appConfig.getName());
    baseConfigs.forEach(artifact -> LOGGER.debug("  {}", artifact));

    if (baseConfigs.size() != 1) {
      throw new GradleException(
          String.format(
              "Configuration '%s' must have exactly one dependency. "
                  + "Use '%s' to add runtime dependencies.",
              APP_CONFIG_NAME, RUNTIME_CONFIG_NAME));
    }
    AppArtifact appArtifact = baseConfigs.iterator().next();
    return appArtifact;
  }

  /** for each dependent artifact read its META-INF looking for quarkus metadata */
  private static AppArtifact handleMetaInf(AppModel.Builder appBuilder, AppDependency dependency) {
    try {
      Path path = dependency.getArtifact().getPaths().getSinglePath();
      try (FileSystem artifactFs = ZipUtils.newFileSystem(path)) {
        Path metaInfPath = artifactFs.getPath(BootstrapConstants.META_INF);
        final Path p = metaInfPath.resolve(BootstrapConstants.DESCRIPTOR_FILE_NAME);
        return Files.exists(p)
            ? processPlatformArtifact(appBuilder, dependency.getArtifact(), p)
            : null;
      }
    } catch (IOException e) {
      throw new GradleException("couldn't read artifact", e);
    }
  }

  /**
   * Search for quarkus metadata and if found augment the AppModel builder. Return any deployment
   * deps.
   */
  private static AppArtifact processPlatformArtifact(
      AppModel.Builder appBuilder, AppArtifact node, Path descriptor) throws IOException {
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

    return new AppArtifact(
        deploymentArtifact.getGroupId(),
        deploymentArtifact.getArtifactId(),
        deploymentArtifact.getClassifier(),
        "jar",
        deploymentArtifact.getVersion());
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
    return new AppArtifact(
        dependency.getGroup(), dependency.getName(), null, "jar", dependency.getVersion());
  }

  private static AppDependency toDependency(ResolvedArtifact dependency) {
    ModuleVersionIdentifier id = dependency.getModuleVersion().getId();
    AppArtifact artifact =
        new AppArtifact(
            id.getGroup(),
            dependency.getName(),
            dependency.getClassifier(),
            dependency.getType(),
            id.getVersion());
    artifact.setPaths(PathsUtils.toPathsCollection(ImmutableList.of(dependency.getFile())));
    return new AppDependency(artifact, "runtime");
  }
}
