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
package org.projectnessie.gc.tool.cli.commands;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.URL;
import java.util.concurrent.Callable;
import picocli.CommandLine;
import picocli.CommandLine.Model.CommandSpec;

@CommandLine.Command(
    name = "show-licenses",
    mixinStandardHelpOptions = true,
    description = "Show 3rd party license information.")
public class ThirdPartyLicenses implements Callable<Integer> {

  @CommandLine.Spec CommandSpec commandSpec;

  @Override
  public Integer call() {
    PrintWriter out = commandSpec.commandLine().getOut();

    out.println();
    out.println("Nessie LICENSE");
    out.println("==============");
    out.println();

    URL url =
        ThirdPartyLicenses.class.getClassLoader().getResource("META-INF/resources/LICENSE.txt");
    try (InputStream in = requireNonNull(url).openConnection().getInputStream()) {
      out.print(new String(in.readAllBytes(), UTF_8));
    } catch (IOException e) {
      throw new RuntimeException("Failed to load resource " + url, e);
    }

    out.println();
    out.println("Nessie NOTICE");
    out.println("=============");
    out.println();

    url = ThirdPartyLicenses.class.getClassLoader().getResource("META-INF/resources/NOTICE.txt");
    try (InputStream in = requireNonNull(url).openConnection().getInputStream()) {
      out.print(new String(in.readAllBytes(), UTF_8));
    } catch (IOException e) {
      throw new RuntimeException("Failed to load resource " + url, e);
    }

    return 0;
  }
}
