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

import static java.util.Arrays.asList;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.List;
import org.jline.terminal.impl.DumbTerminal;
import org.projectnessie.client.api.NessieApi;
import org.projectnessie.nessie.cli.cli.BaseNessieCli;
import org.projectnessie.nessie.cli.cmdspec.CommandSpec;

public class NessieCliTester extends BaseNessieCli implements AutoCloseable {
  private final ByteArrayOutputStream out;

  public NessieCliTester() throws Exception {
    InputStream in = InputStream.nullInputStream();
    out = new ByteArrayOutputStream();

    setTerminal(new DumbTerminal(in, out));
  }

  public List<String> capturedOutput() {
    @SuppressWarnings("resource")
    PrintWriter w = writer();
    w.flush();
    return asList(out.toString().split("\n"));
  }

  @Override
  public void close() {
    nessieApi().ifPresent(NessieApi::close);
  }

  public List<String> execute(CommandSpec spec) throws Exception {
    out.reset();

    NessieCommand<CommandSpec> cmd = CommandsFactory.buildCommandInstance(spec.commandType());
    cmd.execute(this, spec);
    return capturedOutput();
  }
}
