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

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.io.File;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;

import io.quarkus.maven.it.verifier.RunningInvoker;

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

  /**
   * Directory of the relevant quarkus app.
   */
  @Parameter(property = "testDir", required = true)
  private String testDir;

  @Override
  public void execute() throws MojoExecutionException, MojoFailureException {
    if (skipTests) {
      getLog().debug("Tests are skipped. Not starting Nessie Daemon.");
      return;
    }


    getLog().info("Starting Nessie Daemon.");

    try {
      assertThat(new File(testDir)).isDirectory();
      RunningInvoker running = new RunningInvoker(new File(testDir), false);
      final List<String> args = new ArrayList<>(4);
      args.add("quarkus:dev");
      args.add("-Dnessie.mojo.test=true");
      args.add("-Ddebug=false");
      args.add("-Djvm.args=-Xmx128m");
      running.execute(args, Collections.emptyMap());
      if (getHttpResponse()) {
        getLog().info("Nessie Daemon successfully started");
      } else {
        throw new IllegalStateException("Nessie Daemon did not start");
      }
    } catch (Exception e) {
      throw new MojoExecutionException("Failure starting Nessie Daemon", e);
    }
  }

  private static boolean getHttpResponse() {
    AtomicBoolean code = new AtomicBoolean();
    await()
        .pollDelay(1, TimeUnit.SECONDS)
        .atMost(5, TimeUnit.MINUTES).until(() -> {
          try {
            URL url = new URL("http://localhost:19120/");
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            // the default Accept header used by HttpURLConnection is not compatible with RESTEasy negotiation as it uses q=.2
            connection.setRequestProperty("Accept", "text/html, *; q=0.2, */*; q=0.2");
            if (connection.getResponseCode() == 200) {
              code.set(true);
              return true;
            }
            return false;
          } catch (Exception e) {
            return false;
          }
        });
    return code.get();
  }
}
