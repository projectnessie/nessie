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

import org.gradle.api.DefaultTask;
import org.gradle.api.Project;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.file.FileCollection;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.TaskAction;

public class StartTask extends DefaultTask {
  private static final Object lock = new Object();
  private Configuration dataFiles;

  public StartTask() {

  }


  @TaskAction
  public void start() {
    getLogger().info("Starting Quarkus application.");

    synchronized (lock) {
      if (existingApplication()) {
        getLogger().info("Quarkus application already started, incrementing counter.");
        incrementCount();
        return;
      }
      final URL[] urls = getDataFiles().getFiles().stream().map(StartTask::toURL).toArray(URL[]::new);

      final URLClassLoader mirrorCL = new URLClassLoader(urls, this.getClass().getClassLoader());

      final AutoCloseable quarkusApp;
      try {
        Class<?> clazz = mirrorCL.loadClass(QuarkusApp.class.getName());
        Method newApplicationMethod = clazz.getMethod("newApplication", Configuration.class, Project.class);
        quarkusApp = (AutoCloseable) newApplicationMethod.invoke(null, dataFiles, getProject());
      } catch (ReflectiveOperationException e) {
        throw new RuntimeException(e);
      }

      getLogger().info("Quarkus application started.");
      setApplicationHandle(() -> {
        try {
          quarkusApp.close();
        } finally {
          mirrorCL.close();
        }
      });
    }
  }

  @InputFiles
  private FileCollection getDataFiles() {
    return dataFiles;
  }

  public void setConfig(Configuration files) {
    this.dataFiles = files;
  }

  private boolean existingApplication() {
    StopTask task = (StopTask) getProject().getTasks().getByName("quarkus-stop");
    return task.getApplication() != null;
  }

  private void incrementCount() {
    StopTask task = (StopTask) getProject().getTasks().getByName("quarkus-stop");
    task.increment();
  }

  private void setApplicationHandle(AutoCloseable application) {
    // update stop task with this task's closeable

    StopTask task = (StopTask) getProject().getTasks().getByName("quarkus-stop");
    if (task.getApplication() != null) {
      getLogger().warn("StopTask application is not empty!");
    }
    task.setQuarkusApplication(application);
  }

  private static URL toURL(File artifact) {
    try {
      return artifact.toURI().toURL();
    } catch (MalformedURLException e) {
      throw new IllegalArgumentException(e);
    }
  }
}
