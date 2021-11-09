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
package org.projectnessie.tools.contentgenerator.cli;

import java.net.URI;
import java.util.concurrent.Callable;
import org.projectnessie.client.NessieClientBuilder;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.client.http.HttpClientBuilder;
import org.projectnessie.error.BaseNessieClientServerException;
import picocli.CommandLine.Option;

public abstract class AbstractCommand implements Callable<Integer> {

  @Option(
      names = {"-u", "--uri"},
      description = "Nessie API endpoint URI, defaults to http://localhost:19120/api/v1.")
  private URI uri = URI.create("http://localhost:19120/api/v1");

  @Option(
      names = {"-B", "--custom-nessie-client-builder"},
      hidden = true,
      description = "Custom implementation of org.projectnessie.client.NessieClientBuilder.")
  private Class<?> customBuilder;

  public NessieApiV1 createNessieApiInstance() {
    NessieClientBuilder<?> clientBuilder;
    if (customBuilder != null) {
      try {
        clientBuilder =
            (NessieClientBuilder<?>) customBuilder.getDeclaredMethod("builder").invoke(null);
      } catch (Exception e) {
        throw new RuntimeException("Failed to use custom NessieClientBuilder", e);
      }
    } else {
      clientBuilder = HttpClientBuilder.builder();
    }
    clientBuilder.fromSystemProperties();
    if (uri != null) {
      clientBuilder.withUri(uri);
    }
    return clientBuilder.build(NessieApiV1.class);
  }

  /** Convenience method declaration that allows to "just throw" Nessie API exceptions. */
  public abstract void execute() throws BaseNessieClientServerException;

  /**
   * Implements {@link Callable#call()} that just calls {@link #execute()} and returns {@code 0} for
   * the process exit code on success.
   */
  @Override
  public final Integer call() throws Exception {
    execute();
    return 0;
  }
}
