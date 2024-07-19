/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.nessie.cli.commands;

import static org.projectnessie.nessie.cli.cli.BaseNessieCli.STYLE_INFO;
import static org.projectnessie.nessie.cli.cli.BaseNessieCli.STYLE_SUCCESS;

import jakarta.annotation.Nonnull;
import java.io.PrintWriter;
import org.jline.utils.AttributedStringBuilder;
import org.projectnessie.client.api.CommitMultipleOperationsBuilder;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitResponse;
import org.projectnessie.model.Reference;
import org.projectnessie.nessie.cli.cli.BaseNessieCli;
import org.projectnessie.nessie.cli.cmdspec.CommandSpec;
import org.projectnessie.nessie.cli.cmdspec.RefCommandSpec;

public abstract class NessieCommittingCommand<SPEC extends CommandSpec>
    extends NessieCommand<SPEC> {
  protected NessieCommittingCommand() {}

  public final void execute(@Nonnull BaseNessieCli cli, SPEC spec) throws Exception {
    @SuppressWarnings("resource")
    NessieApiV2 api = cli.mandatoryNessieApi();

    String refName = null;
    Reference ref;
    if (spec instanceof RefCommandSpec) {
      refName = ((RefCommandSpec) spec).getRef();
    }
    if (refName == null) {
      ref = cli.getCurrentReference();
    } else {
      ref = api.getReference().refName(refName).get();
    }
    if (!(ref instanceof Branch)) {
      throw new IllegalStateException("Cannot commit to non-branch reference " + ref.getName());
    }

    Branch branch = (Branch) ref;
    CommitMultipleOperationsBuilder commit = api.commitMultipleOperations();
    commit.branch((Branch) ref);

    CommitResponse committed = executeCommitting(cli, spec, branch, commit);
    if (committed == null) {
      return;
    }

    if (cli.getCurrentReference().getName().equals(committed.getTargetBranch().getName())) {
      cli.setCurrentReference(committed.getTargetBranch());
    }

    @SuppressWarnings("resource")
    PrintWriter writer = cli.writer();
    writer.println(
        new AttributedStringBuilder()
            .append("Target branch ")
            .append(committed.getTargetBranch().getName(), STYLE_SUCCESS)
            .append(" is now at commit ")
            .append(committed.getTargetBranch().getHash(), STYLE_INFO)
            .toAnsi(cli.terminal()));
  }

  protected abstract CommitResponse executeCommitting(
      @Nonnull BaseNessieCli cli, SPEC spec, Branch branch, CommitMultipleOperationsBuilder commit)
      throws Exception;
}
