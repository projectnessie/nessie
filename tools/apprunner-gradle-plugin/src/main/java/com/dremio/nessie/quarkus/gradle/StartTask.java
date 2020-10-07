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

import io.quarkus.bootstrap.model.AppArtifactCoords;
import io.quarkus.bootstrap.model.AppModel;
import io.quarkus.bootstrap.resolver.AppModelResolverException;
import io.quarkus.bootstrap.resolver.model.QuarkusModel;
import io.quarkus.bootstrap.utils.BuildToolHelper;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Paths;

import org.apache.maven.artifact.Artifact;
import org.gradle.api.DefaultTask;
import org.gradle.api.Project;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.file.FileCollection;
import org.gradle.api.provider.Property;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.TaskAction;

public class StartTask extends DefaultTask {
  private Configuration dataFiles;
  private Configuration configDeploy;

  public StartTask() {

  }

  @TaskAction
  public void start() {
    getLogger().info("Starting Quarkus application.");

    final URL[] urls = getDataFiles().getFiles().stream().map(StartTask::toURL).toArray(URL[]::new);

    // Use MavenProject classloader as parent classloader as Maven classloader hierarchy is not linear
    final URLClassLoader mirrorCL = new URLClassLoader(urls, this.getClass().getClassLoader());

    final AutoCloseable quarkusApp; // = QuarkusApp.newApplication(dataFiles, configDeploy, getProject());
    try {
      Class<?> clazz = mirrorCL.loadClass(QuarkusApp.class.getName());
      Method newApplicationMethod = clazz.getMethod("newApplication", Configuration.class, Configuration.class, Project.class);
      quarkusApp = (AutoCloseable) newApplicationMethod.invoke(null, dataFiles, configDeploy, getProject());
    } catch (ReflectiveOperationException e) {
      e.printStackTrace();
    }

    getLogger().info("Quarkus application started.");

//    // Make sure classloader is closed too when the app is stopped
//    setApplicationHandle(() -> {
//      try {
//        quarkusApp.close();
//      } finally {
//        mirrorCL.close();
//      }
//    });
  }

  @InputFiles
  private FileCollection getDataFiles() {
    return dataFiles;
  }

  public void setConfig(Configuration files, Configuration configDeploy) {
    this.dataFiles = files;
    this.configDeploy = configDeploy;
  }

//  protected void setApplicationHandle(AutoCloseable application) {
//    final String key = getContextKey();
//    final Object previous = project.getContextValue(key);
//    if (previous != null) {
//      getLogger().warn(String.format("Found a previous application for execution id %s.", getExecutionId()));
//    }
//    project.setContextValue(key, application);
//  }

  private static URL toURL(File artifact) {
    try {
      return artifact.toURI().toURL();
    } catch (MalformedURLException e) {
      throw new IllegalArgumentException(e);
    }
  }
}
