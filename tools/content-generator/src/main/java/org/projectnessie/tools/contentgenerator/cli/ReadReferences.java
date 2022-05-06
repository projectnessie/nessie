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
package org.projectnessie.tools.contentgenerator.cli;

import java.util.List;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.model.Reference;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Spec;

/** Implementation to read all references. */
@Command(name = "refs", mixinStandardHelpOptions = true, description = "Read references")
public class ReadReferences extends AbstractCommand {

  @Spec private CommandSpec spec;

  @Override
  public void execute() {
    try (NessieApiV1 api = createNessieApiInstance()) {
      spec.commandLine().getOut().printf("Reading all references\n\n");
      List<Reference> references = api.getAllReferences().get().getReferences();
      references.forEach(reference -> spec.commandLine().getOut().println(reference));
      spec.commandLine().getOut().printf("\nDone reading all references\n\n");
    }
  }
}
