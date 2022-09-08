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
package org.projectnessie.gc.tool.cli.commands;

import static java.lang.String.format;

import java.time.Instant;
import java.util.stream.Stream;
import org.projectnessie.gc.contents.LiveContentSet;
import org.projectnessie.gc.contents.LiveContentSetsRepository;
import org.projectnessie.gc.files.FileReference;
import org.projectnessie.gc.tool.cli.options.EnvironmentDefaultProvider;
import picocli.CommandLine;
import picocli.CommandLine.ExecutionException;
import picocli.CommandLine.Help.Ansi;

@CommandLine.Command(
    name = "list-deferred",
    mixinStandardHelpOptions = true,
    defaultValueProvider = EnvironmentDefaultProvider.class,
    description =
        "List files collected as deferred deletes, "
            + "must not be used with the in-memory contents-storage.")
public class ListDeferredDeletions extends BaseLiveSetCommand {

  @Override
  protected Integer call(
      LiveContentSet liveContentSet, LiveContentSetsRepository liveContentSetsRepository) {
    if (liveContentSet.status() != LiveContentSet.Status.EXPIRY_SUCCESS) {
      throw new ExecutionException(
          commandSpec.commandLine(),
          "Expected live-set to have status EXPIRY_SUCCESS, but status is "
              + liveContentSet.status());
    }

    out.println(Ansi.AUTO.text("@|bold,underline List of deferred-file-deletions:|@"));
    listLiveContentSets(commandSpec, Stream.of(liveContentSet));

    out.println();

    String format = "%-35s %-60s %s";
    out.println(
        Ansi.AUTO.text(
            format("@|bold,underline " + format + "|@", "Modified", "Base URI", "File path")));
    try (Stream<FileReference> deferredDeletions = liveContentSet.fetchFileDeletions()) {
      deferredDeletions.forEach(
          f ->
              out.println(
                  format(
                      format,
                      Instant.ofEpochMilli(f.modificationTimeMillisEpoch()),
                      f.base(),
                      f.path())));
    }

    out.println("End of listing.");

    return 0;
  }
}
