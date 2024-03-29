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

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import picocli.CommandLine.Option;

class ConnectOptions {
  @Option(
      names = {"-u", "--uri"},
      required = true,
      description = {"REST API endpoint URI to connect to.", "See 'HELP CONNECT' in the REPL."})
  URI uri;

  @Option(
      names = "--client-name",
      description = {
        "Name of the client implementation to use, defaults to HTTP suitable for Nessie REST API.",
        "See https://projectnessie.org/nessie-latest/client_config/ for the 'nessie.client-builder-name' option."
      })
  String clientName;

  @Option(
      names = {"-o", "--client-option"},
      description = {
        "Parameters to configure the REST client.",
        "See https://projectnessie.org/nessie-latest/client_config/"
      },
      split = ",",
      arity = "0..*")
  Map<String, String> clientOptions = new HashMap<>();

  @Option(
      names = {"-r", "--initial-reference"},
      description = "Name of the Nessie reference to use.")
  String initialReference;
}
