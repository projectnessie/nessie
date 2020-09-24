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
package com.dremio.nessie.tools.daemon;

import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.List;

import org.apache.maven.artifact.Artifact;
import org.apache.maven.artifact.DefaultArtifact;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugin.descriptor.PluginDescriptor;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.project.MavenProject;

/**
 * Starting Quarkus daemon.
 */
@Mojo(name = "start", defaultPhase = LifecyclePhase.PRE_INTEGRATION_TEST)
public class StartMojo extends AbstractMojo {

  private static final String DAEMON_SERVER_HOLDER = "com.dremio.nessie.tools.daemon.ServerHolder";
  /**
   * Whether you should skip while running in the test phase (default is false).
   */
  @Parameter(property = "skipTests", required = false, defaultValue = "false")
  private Boolean skipTests;

  @Parameter(defaultValue = "${project}", readonly = true, required = true)
  private MavenProject project;

  @Parameter(defaultValue = "${plugin}", readonly = true)
  private PluginDescriptor pluginDescriptor;

  private URLClassLoader buildClassLoader() throws MalformedURLException {
    Artifact projectArtifact = project.getArtifact();
    String quarkusArtifact = new DefaultArtifact(projectArtifact.getGroupId(),
                                                 "nessie-quarkus",
                                                 projectArtifact.getVersion(),
                                                 null,
                                                 "jar",
                                                 "runner",
                                                 null).toString();
    List<Artifact> artifacts = pluginDescriptor.getArtifacts();
    //filter plugin artifacts to remove all artifacts not related to the quarkus server (or the classes in this package)
    URL[] pathUrls = artifacts.stream()
                              .filter(x -> isFromQuarkus(x, quarkusArtifact) || x.equals(pluginDescriptor.getPluginArtifact()))
                              .map(x -> {
                                try {
                                  return x.getFile().toURI().toURL();
                                } catch (MalformedURLException e) {
                                  throw new RuntimeException(e);
                                }
                              })
                              .toArray(URL[]::new);
    getLog().debug("urls for URLClassLoader: " + Arrays.asList(pathUrls));

    return new URLClassLoader(pathUrls, ClassLoader.getSystemClassLoader());
  }

  private boolean isFromQuarkus(Artifact artifact, String quarkusArtifact) {
    return artifact.getDependencyTrail().stream().anyMatch(quarkusArtifact::equals);
  }

  @Override
  public void execute() throws MojoExecutionException, MojoFailureException {
    if (skipTests) {
      getLog().debug("Tests are skipped. Not starting Nessie Daemon.");
      return;
    }

    getLog().info("Starting Nessie Daemon.");

    try {
      final ClassLoader classLoader = buildClassLoader();

      Class<?> holderClazz = Class.forName(DAEMON_SERVER_HOLDER, true, classLoader);
      Object holder =  holderClazz.getConstructor(ClassLoader.class).newInstance(classLoader);
      holderClazz.getMethod("start").invoke(holder);

      project.setContextValue("quarkusServerHolder", holder);
      project.setContextValue("quarkusServerHolderClass", holderClazz);
      Thread.sleep(2000L); //wait for Quarkus to start up.
      boolean isRunning = (boolean) holderClazz.getMethod("isRunning").invoke(holder);
      if (!isRunning) {
        Exception e = (Exception) holderClazz.getMethod("error").invoke(holder);
        throw new MojoExecutionException("Quarkus thread failed", e);
      }
    } catch (Exception e) {
      throw new MojoExecutionException("Failure starting Nessie Daemon", e instanceof MojoExecutionException ? e.getCause() : e);
    }
  }
}
