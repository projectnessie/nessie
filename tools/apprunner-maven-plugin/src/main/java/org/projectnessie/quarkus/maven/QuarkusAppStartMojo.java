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

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugin.descriptor.PluginDescriptor;
import org.apache.maven.plugins.annotations.Component;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.plugins.annotations.ResolutionScope;
import org.apache.maven.project.MavenProject;
import org.apache.maven.toolchain.ToolchainManager;
import org.eclipse.aether.RepositorySystem;
import org.eclipse.aether.RepositorySystemSession;
import org.eclipse.aether.artifact.Artifact;
import org.eclipse.aether.artifact.DefaultArtifact;
import org.eclipse.aether.repository.RemoteRepository;
import org.eclipse.aether.resolution.ArtifactRequest;
import org.eclipse.aether.resolution.ArtifactResolutionException;
import org.eclipse.aether.resolution.ArtifactResult;
import org.projectnessie.quarkus.runner.JavaVM;
import org.projectnessie.quarkus.runner.ProcessHandler;

/**
 * Starting Quarkus application.
 */
@Mojo(name = "start", requiresDependencyResolution = ResolutionScope.NONE, threadSafe = true)
public class QuarkusAppStartMojo extends AbstractQuarkusAppMojo {
  /*
   * Execution lock across multiple executions of quarkus app.
   *
   * Quarkus application might modify system properties to reflect dynamic
   * configuration values. However it is not possible for each execution to have
   * its own properties.
   * Lock is designed to make sure only one application is started and system properties
   * retrieved, while still allowing multiple applications to run concurrently.
   *
   * TODO: the lock is not truly global since plugins are maintained in separate classloaders.
   * It should be changed to something attached to the Maven session instead.
   */
  private static final Object START_LOCK = new Object();

  /**
   * The entry point to Aether, i.e. the component doing all the work.
   *
   * @component
   */
  @Component
  private RepositorySystem repoSystem;

  @Component
  private ToolchainManager toolchainManager;

  /**
   * The current repository/network configuration of Maven.
   */
  @Parameter(defaultValue = "${repositorySystemSession}", readonly = true)
  private RepositorySystemSession repoSession;

  /**
   * The project's remote repositories to use for the resolution of plugins and their dependencies.
   */
  @Parameter(defaultValue = "${project.remotePluginRepositories}", readonly = true)
  private List<RemoteRepository> remoteRepos;

  /**
   * The plugin descriptor.
   */
  @Parameter(defaultValue = "${plugin}", readonly = true)
  private PluginDescriptor pluginDescriptor;

  /**
   * The application artifact id.
   *
   * <p>Needs to be present as a plugin dependency, if {@link #executableJar} is not set.
   * <p>Mutually exclusive with {@link #executableJar}</p>
   *
   * <p>Supported format is groupId:artifactId[:type[:classifier]]:version
   */
  @Parameter(property = "nessie.apprunner.appArtifactId")
  private String appArtifactId;

  /**
   * Environment variable configuration properties.
   */
  @Parameter
  private Properties systemProperties = new Properties();

  /**
   * Properties to get from Quarkus running application.
   *
   * <p>The property key is the name of the build property to set, the value is
   * the name of the quarkus configuration key to get.
   */
  @Parameter
  private Properties environment;

  @Parameter
  private List<String> arguments;

  @Parameter
  private List<String> jvmArguments;

  @Parameter(defaultValue = "11")
  private int javaVersion;

  /**
   * The path to the executable jar to run.
   * <p>If in doubt and not using the plugin inside the Nessie source tree, use {@link #appArtifactId}.</p>
   * <p>Mutually exclusive with {@link #appArtifactId}</p>
   */
  @Parameter
  private String executableJar;

  @Parameter(defaultValue = "quarkus.http.test-port")
  private String httpListenPortProperty;

  @Parameter(defaultValue = "quarkus.http.test-url")
  private String httpListenUrlProperty;

  @Parameter(defaultValue = "${project.builddir}/nessie-quarkus")
  private String workingDirectory;

  @Parameter
  private long timeToListenUrlMillis;

  @Parameter
  private long timeToStopMillis;

  static String noJavaVMMessage(int version) {
    return String.format("Could not find a Java-VM for Java version %d. "
            + "Either configure a type=jdk in Maven's toolchain with version=%d or "
            + "set the Java-Home for a compatible JVM using the environment variable JDK%d_HOME or "
            + "JAVA%d_HOME.",
        version, version, version, version);
  }

  @Override
  public void execute() throws MojoExecutionException, MojoFailureException {
    if (isSkipped()) {
      getLog().debug("Execution is skipped");
      return;
    }

    getLog().debug(String.format("Searching for Java %d ...", javaVersion));
    String javaExecutable =
        toolchainManager.getToolchains(getSession(), "jdk", Collections.singletonMap("version", Integer.toString(javaVersion)))
            .stream()
            .map(tc -> tc.findTool("java"))
            .findFirst()
            .orElseGet(() -> {
              getLog().debug(String.format("... using JavaVM as Maven toolkit returned no toolchain "
                  + "for type==jdk and version==%d", javaVersion));
              JavaVM javaVM = JavaVM.findJavaVM(javaVersion);
              return javaVM != null ? javaVM.getJavaExecutable().toString() : null;
            });
    if (javaExecutable == null) {
      throw new MojoExecutionException(noJavaVMMessage(javaVersion));
    }
    getLog().debug(String.format("Using javaExecutable %s", javaExecutable));

    Path workDir = Paths.get(workingDirectory);
    if (!Files.isDirectory(workDir)) {
      try {
        Files.createDirectories(workDir);
      } catch (IOException e) {
        throw new MojoExecutionException(String.format("Failed to create working directory %s", workingDirectory), e);
      }
    }

    String execJar = executableJar;
    if (execJar == null && appArtifactId == null) {
      if (getProject().getGroupId().equals("org.projectnessie")) {
        getLog().debug("Nessie source-tree build.");
        // Special case handling for Nessie source-tree builds.
        // Find the root-project (org.projectnessie:nessie) from the current project and resolve the 'quarkus-run.jar' from there.
        // Unfortunately, it's not possible to declare a Maven project property to ease this/make it clearer, because project property
        // evaluation, even for parent-poms, happens in the context of the currently executed project.
        //
        // To use this Maven plugin within the Nessie source tree do specify neither appArtifactId nor executableJar.
        // Using this Maven plugin outside the Nessie source tree requires using either appArtifactId (preferred) or executableJar.
        for (MavenProject p = getProject().getParent(); p.getGroupId().equals("org.projectnessie"); p = p.getParent()) {
          if (p.getArtifactId().equals("nessie")) {
            getLog().info("Using quarkus-run.jar from org.projectnessie source tree build");
            execJar = p.getBasedir().toPath()
                .resolve(Paths.get("servers", "quarkus-server", "target", "quarkus-app", "quarkus-run.jar"))
                .toString();
            break;
          }
        }
      }
      if (execJar == null) {
        throw new MojoExecutionException("Either appArtifactId or executableJar config option must be specified, prefer appArtifactId");
      }
    }
    if (execJar == null) {
      Artifact artifact = new DefaultArtifact(appArtifactId);
      ArtifactRequest artifactRequest = new ArtifactRequest(artifact, remoteRepos, null);
      try {
        ArtifactResult result = repoSystem.resolveArtifact(repoSession, artifactRequest);
        execJar = result.getArtifact().getFile().toString();
      } catch (ArtifactResolutionException e) {
        throw new MojoExecutionException(String.format("Failed to resolve artifact %s", appArtifactId), e);
      }
    } else if (appArtifactId != null) {
      throw new MojoExecutionException("The options appArtifactId and executableJar are mutually exclusive");
    }

    List<String> command = new ArrayList<>();
    command.add(javaExecutable);
    if (jvmArguments != null) {
      command.addAll(jvmArguments);
    }
    if (systemProperties != null) {
      systemProperties.forEach((k, v) -> command.add(String.format("-D%s=%s", k.toString(), v.toString())));
    }
    command.add("-Dquarkus.http.port=0");
    command.add("-jar");
    command.add(execJar);
    if (arguments != null) {
      command.addAll(arguments);
    }

    getLog().info(String.format("Starting process: %s, additional env: %s",
        String.join(" ", command),
        environment != null
            ? environment.entrySet().stream().map(e -> String.format("%s=%s", e.getKey(), e.getValue())).collect(Collectors.joining(", "))
            : "<none>"));

    ProcessBuilder processBuilder = new ProcessBuilder()
        .command(command);
    if (environment != null) {
      environment.forEach((k, v) -> processBuilder.environment().put(k.toString(), v.toString()));
    }
    processBuilder.directory(workDir.toFile());

    try {
      ProcessHandler processHandler = new ProcessHandler();
      if (timeToListenUrlMillis > 0L) {
        processHandler.setTimeToListenUrlMillis(timeToListenUrlMillis);
      }
      if (timeToStopMillis > 0L) {
        processHandler.setTimeStopMillis(timeToStopMillis);
      }
      processHandler.start(processBuilder);

      setApplicationHandle(processHandler);

      String listenUrl = processHandler.getListenUrl();

      Properties projectProperties = getProject().getProperties();
      projectProperties.setProperty(httpListenUrlProperty, listenUrl);
      projectProperties.setProperty(httpListenPortProperty, Integer.toString(URI.create(listenUrl).getPort()));

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new MojoExecutionException(String.format("Process-start interrupted: %s", command), e);
    } catch (Exception e) {
      throw new MojoExecutionException(String.format("Failed to start the process %s", command), e);
    }
  }
}
