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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

import org.apache.maven.artifact.Artifact;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugin.descriptor.PluginDescriptor;
import org.apache.maven.plugins.annotations.Component;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.plugins.annotations.ResolutionScope;
import org.apache.maven.project.MavenProject;
import org.eclipse.aether.RepositorySystem;
import org.eclipse.aether.RepositorySystemSession;

import io.quarkus.bootstrap.model.AppArtifactCoords;

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

  /**
   * Application configuration properties.
   */
  @Parameter
  private Properties applicationProperties;

  @Override
  public void execute() throws MojoExecutionException, MojoFailureException {
    if (isSkipped()) {
      getLog().debug("Execution is skipped");
      return;
    }

    final AppArtifactCoords appCoords = AppArtifactCoords.fromString(appArtifactId);

    // Check that the artifact is present as it might cause some classloader
    // confusion if not
    boolean appArtifactPresent = pluginDescriptor.getArtifacts().stream()
        .map(artifact -> new AppArtifactCoords(artifact.getGroupId(), artifact.getArtifactId(),
            artifact.getClassifier(), artifact.getType(), artifact.getVersion()))
        .filter(coords -> coords.equals(appCoords)).findAny().isPresent();

    if (!appArtifactPresent) {
      throw new MojoExecutionException(
          String.format("Artifact %s not found in plugin dependencies", appCoords));
    }

    getLog().info("Starting Quarkus application.");

    final URL[] urls = pluginDescriptor.getArtifacts().stream().map(QuarkusAppStartMojo::toURL).toArray(URL[]::new);

    // Use MavenProject classloader as parent classloader as Maven classloader hierarchy is not linear
    final URLClassLoader mirrorCL = new URLClassLoader(urls, MavenProject.class.getClassLoader());

    final AutoCloseable quarkusApp;
    try {
      Class<?> clazz = mirrorCL.loadClass(QuarkusApp.class.getName());
      Method newApplicationMethod = clazz.getMethod("newApplication", MavenProject.class,
          RepositorySystem.class, RepositorySystemSession.class, String.class, Properties.class, Collection.class);
      quarkusApp = (AutoCloseable) newApplicationMethod.invoke(null, getProject(), repoSystem,
          repoSession, appArtifactId, applicationProperties,
          Collections.singleton(pluginDescriptor.getPluginArtifact().getFile().toPath()));
    } catch (InvocationTargetException e) {
      throw new MojoExecutionException("Cannot create an isolated quarkus application", e.getCause());
    } catch (ReflectiveOperationException e) {
      throw new MojoExecutionException("Cannot create an isolated quarkus application", e);
    }

    getLog().info("Quarkus application started.");

    // Make sure classloader is closed too when the app is stopped
    setApplicationHandle(() -> {
      try {
        quarkusApp.close();
      } finally {
        mirrorCL.close();
      }
    });
  }

  private static URL toURL(Artifact artifact) {
    try {
      return artifact.getFile().toURI().toURL();
    } catch (MalformedURLException e) {
      throw new IllegalArgumentException(e);
    }
  }
}
