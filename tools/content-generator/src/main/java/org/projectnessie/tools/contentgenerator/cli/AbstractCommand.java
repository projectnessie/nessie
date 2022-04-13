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

import java.util.concurrent.Callable;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.error.BaseNessieClientServerException;
import picocli.CommandLine;
import picocli.CommandLine.Option;

public abstract class AbstractCommand implements Callable<Integer> {

  @CommandLine.ParentCommand private ContentGenerator<NessieApiV1> parent;

  @Option(
      names = {"-v", "--verbose"},
      description = "Produce verbose output (if possible)")
  private boolean verbose = false;

  protected boolean isVerbose() {
    return verbose;
  }

  public NessieApiV1 createNessieApiInstance() {
    return parent.createNessieApiInstance();
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
