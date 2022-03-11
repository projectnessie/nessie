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
package org.projectnessie.quarkus.cli;

import com.google.protobuf.ByteString;
import java.util.stream.Collectors;
import org.projectnessie.versioned.GetNamedRefsParams;
import org.projectnessie.versioned.GetNamedRefsParams.RetrieveOptions;
import org.projectnessie.versioned.ReferenceInfo;
import org.projectnessie.versioned.persist.adapter.RepoDescription;
import picocli.CommandLine.Command;

@Command(
    name = "info",
    mixinStandardHelpOptions = true,
    description = "Nessie repository information")
public class NessieInfo extends BaseCommand {

  @Override
  public Integer call() throws Exception {
    warnOnInMemory();

    ReferenceInfo<ByteString> refInfo =
        databaseAdapter.namedRef(
            serverConfig.getDefaultBranch(),
            GetNamedRefsParams.builder()
                .branchRetrieveOptions(RetrieveOptions.COMMIT_META)
                .tagRetrieveOptions(RetrieveOptions.COMMIT_META)
                .build());

    RepoDescription repoDesc = databaseAdapter.fetchRepositoryDescription();

    spec.commandLine()
        .getOut()
        .printf(
            "%n"
                //
                + "No-ancestor hash:                  %s%n"
                + "Default branch head commit ID:     %s%n"
                + "Default branch commit count:       %s%n"
                + "Repository description version:    %d%n"
                + "Repository description properties: %s%n"
                + "%n"
                + "From configuration:%n"
                + "-------------------%n"
                + "Version-store type:                %s%n"
                + "Default branch:                    %s%n",
            databaseAdapter.noAncestorHash().asString(),
            refInfo.getHash().asString(),
            refInfo.getCommitSeq(),
            repoDesc.getRepoVersion(),
            repoDesc.getProperties().entrySet().stream()
                .map(e -> String.format("%-30s = %s", e.getKey(), e.getValue()))
                .collect(Collectors.joining("\n                                   ")),
            versionStoreConfig.getVersionStoreType(),
            serverConfig.getDefaultBranch());

    return 0;
  }
}
