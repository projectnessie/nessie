/*
 * Copyright (C) 2022 Dremio
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
package org.projectnessie.gc.tool.cli.options;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import picocli.CommandLine;
import picocli.CommandLine.ExecutionException;
import picocli.CommandLine.Model.CommandSpec;

public class LiveSetIdOptions {

  @CommandLine.Option(
      names = {"-l", "--live-set-id"},
      description = "ID of the live content set.")
  private UUID liveSetId;

  @CommandLine.Option(
      names = {"-L", "--read-live-set-id-from"},
      description = "The file to read the live-set-id from.")
  private Path liveSetIdFile;

  public UUID getLiveSetId(CommandSpec commandSpec) {
    if (liveSetId != null) {
      return liveSetId;
    }

    try {
      return UUID.fromString(Files.readAllLines(liveSetIdFile).get(0));
    } catch (IOException e) {
      throw new ExecutionException(
          commandSpec.commandLine(), "Cannot read live-set-id from file " + liveSetIdFile, e);
    }
  }
}
