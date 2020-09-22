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

import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.jutils.jprocesses.JProcesses;
import org.jutils.jprocesses.model.ProcessInfo;

/**
 * Stop dremio daemon.
 */
@Mojo(name = "stop", defaultPhase = LifecyclePhase.POST_INTEGRATION_TEST)
public class StopMojo extends AbstractMojo {
  /**
   * Whether you should skip while running in the test phase (default is false).
   */
  @Parameter(property = "skipTests", required = false, defaultValue = "false")
  private Boolean skipTests;

  /**
   * Mojo execution.
   */
  public void execute() throws MojoExecutionException, MojoFailureException {
    getLog().info("Stopping Nessie Daemon.");
    boolean stopped = false;
    try {
      for (ProcessInfo pi : JProcesses.getProcessList()) {
        for (String part : new String[]{"-Dnessie.mojo.test=true"}) {
          if (pi.getCommand().contains(part)) {
            JProcesses.killProcess(Integer.parseInt(pi.getPid()));
            getLog().info("Successfully stopped Nessie Daemon on pid " + pi.getPid());
            stopped = true;
            break;
          }
        }
      }
      if (!stopped) {
        getLog().warn("A running Nessie Daemon was not found. Please ensure Nessie was stopped");
      }
    } catch (Exception e) {
      throw new MojoExecutionException("Failure stopping Nessie Daemon", e);
    }
  }

}
