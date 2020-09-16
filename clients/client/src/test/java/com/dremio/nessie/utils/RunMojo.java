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
package com.dremio.nessie.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.maven.shared.invoker.MavenInvocationException;

import io.quarkus.maven.it.RunAndCheckMojoTestBase;
import io.quarkus.maven.it.verifier.RunningInvoker;

/**
 * Adaptation of Quarkus Mojo to test a Quarkus service in a separate JVM.
 *
 * <p>
 *   This allows running a quarkus server in a new jvm/classloader which doesn't clash with the Spark tests classpath.
 * </p>
 */
public class RunMojo extends RunAndCheckMojoTestBase {

  public RunMojo() {
    Path root = Paths.get(System.getProperty("user.dir")).getParent().getParent();
    testDir = Paths.get(root.toString(), "servers", "quarkus-server").toFile();
  }

  /**
   * Start the nessie service.
   */
  public void start() throws IOException {
    try {
      super.run(false, "-Dmaven.home=" + System.getProperty("maven.home"),
                "-Dnessie.version.store.jgit.type=INMEMORY");
    } catch (MavenInvocationException e) {
      throw new IOException(e);
    }
    getHttpResponse("", 200);
  }

  /**
   * Check to see if Nessie has come up and is available. Fail after 5 minutes and error out.
   */
  public static boolean getHttpResponse(String path, int expectedStatus) {
    AtomicBoolean code = new AtomicBoolean();
    await()
        .pollDelay(1, TimeUnit.SECONDS)
        .atMost(5, TimeUnit.MINUTES).until(() -> {
          try {
            URL url = new URL("http://localhost:19120" + ((path.startsWith("/") ? path : "/" + path)));
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            // the default Accept header used by HttpURLConnection is not compatible with RESTEasy negotiation as it uses q=.2
            connection.setRequestProperty("Accept", "text/html, *; q=0.2, */*; q=0.2");
            if (connection.getResponseCode() == expectedStatus) {
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

  @Override
  protected void run(boolean performCompile, String... options) throws MavenInvocationException {
    assertThat(testDir).isDirectory();
    running = new RunningInvoker(testDir, false);
    final List<String> args = new ArrayList<>(2 + options.length);
    if (performCompile) {
      args.add("compile");
    }
    args.add("quarkus:dev");
    boolean hasDebugOptions = false;
    for (String option : options) {
      args.add(option);
      if (option.trim().startsWith("-Ddebug=") || option.trim().startsWith("-Dsuspend=")) {
        hasDebugOptions = true;
      }
    }
    if (!hasDebugOptions) {
      // if no explicit debug options have been specified, let's just disable debugging
      args.add("-Ddebug=false");
    }

    //we need to limit the memory consumption, as we can have a lot of these processes
    //running at once, if they add default to 75% of total mem we can easily run out
    //of physical memory as they will consume way more than what they need instead of
    //just running GC
    args.add("-Djvm.args=-Xmx128m");
    running.execute(args, Collections.emptyMap());
  }

  public void stop() throws IOException {
    super.cleanup();
  }
}
