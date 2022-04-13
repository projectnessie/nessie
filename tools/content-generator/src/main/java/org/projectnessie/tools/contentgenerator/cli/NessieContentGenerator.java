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

import com.google.common.annotations.VisibleForTesting;
import java.io.PrintWriter;
import java.net.URI;
import org.projectnessie.client.NessieClientBuilder;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.client.http.HttpClientBuilder;
import org.projectnessie.client.http.HttpClientException;
import org.projectnessie.error.BaseNessieClientServerException;
import picocli.CommandLine;

public class NessieContentGenerator extends ContentGenerator<NessieApiV1> {

  @CommandLine.Option(
      names = {"-u", "--uri"},
      scope = CommandLine.ScopeType.INHERIT,
      description = "Nessie API endpoint URI, defaults to http://localhost:19120/api/v1.")
  private URI uri = URI.create("http://localhost:19120/api/v1");

  public static void main(String[] arguments) {
    System.exit(runMain(arguments));
  }

  @Override
  public NessieApiV1 createNessieApiInstance() {
    NessieClientBuilder<?> clientBuilder = HttpClientBuilder.builder();
    clientBuilder.fromSystemProperties();
    if (uri != null) {
      clientBuilder.withUri(uri);
    }
    return clientBuilder.build(NessieApiV1.class);
  }

  @VisibleForTesting
  public static int runMain(String[] arguments) {
    return runMain(null, arguments);
  }

  @VisibleForTesting
  public static int runMain(PrintWriter out, String[] arguments) {
    CommandLine commandLine =
        new CommandLine(new NessieContentGenerator())
            .setExecutionExceptionHandler(
                (ex, cmd, parseResult) -> {
                  if (ex instanceof BaseNessieClientServerException
                      || ex instanceof HttpClientException) {
                    // Just print the exception w/o the stack trace for Nessie related exceptions
                    cmd.getErr().println(cmd.getColorScheme().errorText(ex.toString()));
                  } else {
                    // Print the full stack trace in all other cases.
                    cmd.getErr().println(cmd.getColorScheme().richStackTraceString(ex));
                  }
                  return cmd.getExitCodeExceptionMapper() != null
                      ? cmd.getExitCodeExceptionMapper().getExitCode(ex)
                      : cmd.getCommandSpec().exitCodeOnExecutionException();
                });
    if (null != out) {
      commandLine = commandLine.setOut(out);
    }
    return commandLine.execute(arguments);
  }
}
