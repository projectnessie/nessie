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
package com.dremio.tools.daemon;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.io.IOUtils;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;

/**
 * Starting Quarkus daemon.
 */
@Mojo(name = "start", defaultPhase = LifecyclePhase.PRE_INTEGRATION_TEST)
public class StartMojo extends AbstractMojo {

  /**
   * Whether you should skip while running in the test phase (default is false).
   */
  @Parameter(property = "skipTests", required = false, defaultValue = "false")
  private Boolean skipTests;


  @Override
  public void execute() throws MojoExecutionException, MojoFailureException {
    if (skipTests) {
      getLog().debug("Tests are skipped. Not starting Nessie Daemon.");
      return;
    }


    getLog().info("Starting Nessie Daemon.");

    try {
      String app = Thread.currentThread().getContextClassLoader().getResource("io/quarkus/runner/ApplicationImpl.class").getPath();
      String appPath = app.replace("file:", "").split("!")[0];
      ProcessBuilder builder = new ProcessBuilder();
      builder.command("java", "-jar", appPath);
      builder.environment().put("QUARKUS_PROFILE", "test");
      Process process = builder.start();

      ExecutorService executor = Executors.newSingleThreadExecutor();
      executor.submit(() -> {
        while (!Thread.currentThread().isInterrupted()) {
          try {
            IOUtils.copy(process.getInputStream(), System.out);
            IOUtils.copy(process.getErrorStream(), System.err);
          } catch (IOException exception) {
            // pass
          }
        }
      });
      ServerHolder.setDaemon(process);
      ServerHolder.setExecutor(executor);
    } catch (Exception e) {
      e.printStackTrace();
      throw new MojoExecutionException("Failure starting Nessie Daemon", e);
    }
  }

}
