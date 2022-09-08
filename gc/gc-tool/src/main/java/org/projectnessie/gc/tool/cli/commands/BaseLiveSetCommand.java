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

import org.projectnessie.gc.contents.LiveContentSet;
import org.projectnessie.gc.contents.LiveContentSetNotFoundException;
import org.projectnessie.gc.contents.LiveContentSetsRepository;
import org.projectnessie.gc.tool.cli.Closeables;
import org.projectnessie.gc.tool.cli.options.LiveSetIdOptions;
import picocli.CommandLine;
import picocli.CommandLine.PicocliException;

public abstract class BaseLiveSetCommand extends BaseRepositoryCommand {

  @CommandLine.ArgGroup(multiplicity = "1")
  LiveSetIdOptions liveSetIdOptions;

  @Override
  protected Integer call(
      Closeables closeables, LiveContentSetsRepository liveContentSetsRepository) {
    LiveContentSet liveContentSet;
    try {
      liveContentSet =
          liveContentSetsRepository.getLiveContentSet(liveSetIdOptions.getLiveSetId(commandSpec));
    } catch (LiveContentSetNotFoundException e) {
      throw new PicocliException(e.getMessage());
    }

    return call(liveContentSet, liveContentSetsRepository);
  }

  protected abstract Integer call(
      LiveContentSet liveContentSet, LiveContentSetsRepository liveContentSetsRepository);
}
