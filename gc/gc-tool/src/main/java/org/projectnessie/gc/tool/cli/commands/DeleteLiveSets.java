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

import java.util.stream.Stream;
import org.projectnessie.gc.contents.LiveContentSet;
import org.projectnessie.gc.contents.LiveContentSetsRepository;
import org.projectnessie.gc.tool.cli.options.EnvironmentDefaultProvider;
import picocli.CommandLine;
import picocli.CommandLine.Help.Ansi;

@CommandLine.Command(
    name = "delete",
    mixinStandardHelpOptions = true,
    defaultValueProvider = EnvironmentDefaultProvider.class,
    description = "Delete a live-set, " + "must not be used with the in-memory contents-storage.")
public class DeleteLiveSets extends BaseLiveSetCommand {

  @Override
  protected Integer call(
      LiveContentSet liveContentSet, LiveContentSetsRepository liveContentSetsRepository) {
    out.println("Deleting live content set " + liveContentSet.id());
    listLiveContentSets(commandSpec, Stream.of(liveContentSet));

    liveContentSet.delete();

    out.println(
        Ansi.AUTO.text(
            "@|bold,green Live content set " + liveContentSet.id() + "successfully deleted.|@"));

    return 0;
  }
}
