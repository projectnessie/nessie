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

import static java.util.Map.Entry.comparingByKey;

import java.io.PrintWriter;
import java.time.Duration;
import java.util.Map;
import org.projectnessie.versioned.persist.adapter.RepoMaintenanceParams;
import picocli.CommandLine.Command;

@Command(
    name = "maintenance",
    mixinStandardHelpOptions = true,
    description = "Database adapter maintenance")
public class RepoMaintenance extends BaseCommand {

  @Override
  public Integer call() {
    warnOnInMemory();

    PrintWriter out = spec.commandLine().getOut();

    out.println("Running repository maintenance...");

    long t0 = System.nanoTime();
    Map<String, Map<String, String>> statistics =
        databaseAdapter.repoMaintenance(RepoMaintenanceParams.builder().build());
    Duration duration = Duration.ofNanos(System.nanoTime() - t0);

    out.printf("Finished after %s%n", duration);

    statistics.entrySet().stream()
        .sorted(comparingByKey())
        .forEach(
            top -> {
              out.printf("%nCategory '%s':%n", top.getKey());
              top.getValue().entrySet().stream()
                  .sorted(comparingByKey())
                  .map(e -> String.format("    %-30s: %s", e.getKey(), e.getValue()))
                  .forEach(out::println);
            });

    return 0;
  }
}
