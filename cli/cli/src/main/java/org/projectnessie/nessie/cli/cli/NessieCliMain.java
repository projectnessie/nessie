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
package org.projectnessie.nessie.cli.cli;

import picocli.CommandLine;

public class NessieCliMain {

  public static void main(String[] arguments) {

    // There's no easy, better way :(
    // Setting the usage-width to 100 chars so that URLs are not line-wrapped.
    System.setProperty("picocli.usage.width", "100");

    CommandLine commandLine = new CommandLine(new NessieCliImpl());
    try {
      System.exit(commandLine.execute(arguments));
    } finally {
      commandLine.getOut().flush();
      commandLine.getErr().flush();
    }
  }
}
