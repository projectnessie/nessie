/*
 * Copyright (C) 2023 Dremio
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

import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.ImmutableCommitMeta;
import picocli.CommandLine;

public abstract class CommittingCommand extends AbstractCommand {

  @CommandLine.Option(
      names = {"--author"},
      defaultValue = "${USER}",
      description =
          "Commit author for changes made by this command (optional, defaults to the USER env. var.)")
  private String author;

  @CommandLine.Option(
      names = {"-m", "--message"},
      description = "Commit message to use (auto-generated if not set).")
  private String message;

  protected CommitMeta commitMetaFromMessage(String defaultMessage) {
    ImmutableCommitMeta.Builder builder =
        CommitMeta.builder().message(message == null ? defaultMessage : message);

    if (author != null && !author.isEmpty()) {
      builder.author(author);
    }

    return builder.build();
  }
}
