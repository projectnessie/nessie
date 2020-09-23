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

import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Consumer;

import org.apache.maven.artifact.Artifact;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugin.descriptor.PluginDescriptor;
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

  private URLClassLoader buildClassLoader() throws MalformedURLException {

    List<Artifact> artifacts = ((PluginDescriptor) getPluginContext().get("pluginDescriptor")).getArtifacts();
    URL[] pathUrls = artifacts.stream()
                              .filter(x -> {
                                return x.hasClassifier() && x.getClassifier().equals("runner");
                              })
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

  private final Consumer<Integer> exitHandler = new Consumer<Integer>() {
    @Override
    public void accept(Integer integer) {
      //not interested in the exit value
    }
  };

  @Override
  public void execute() throws MojoExecutionException, MojoFailureException {
    if (skipTests) {
      getLog().debug("Tests are skipped. Not starting Nessie Daemon.");
      return;
    }

    getLog().info("Starting Nessie Daemon.");

    try {
      final ClassLoader classLoader = buildClassLoader();

      ExecutorService executor = Executors.newSingleThreadExecutor();
      Future<?> job = executor.submit(() -> {
        final ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(classLoader);
        try {
          Class<?> appClass = Class.forName("io.quarkus.runtime.ApplicationLifecycleManager",
                                            true,
                                            Thread.currentThread().getContextClassLoader());
          //getting the method with a Consumer<Integer> is a bit fiddly. just look by name
          Optional<Method> method = Arrays.stream(appClass.getMethods())
                                          .filter(x -> x.getName().equals("setDefaultExitCodeHandler")).findFirst();
          method.orElseThrow(() -> new RuntimeException("couldn't find method")).invoke(null, exitHandler);
          Class<?> quarkusClass = Class.forName("io.quarkus.runtime.Quarkus", true, Thread.currentThread().getContextClassLoader());
          quarkusClass.getMethod("run", String[].class).invoke(null, new Object[] {new String[] {"-Dquarkus.profile=test"}});
        } catch (ReflectiveOperationException e) {
          throw new RuntimeException(e);
        } finally {
          Thread.currentThread().setContextClassLoader(originalClassLoader);
        }
      });
      ServerHolder.setExecutor(executor);
      ServerHolder.setDaemon(job);
      ServerHolder.setClassLoader(classLoader);
      Thread.sleep(1000L); //wait for Quarkus to start up.
    } catch (Exception e) {
      e.printStackTrace();
      throw new MojoExecutionException("Failure starting Nessie Daemon", e);
    }
  }
}
