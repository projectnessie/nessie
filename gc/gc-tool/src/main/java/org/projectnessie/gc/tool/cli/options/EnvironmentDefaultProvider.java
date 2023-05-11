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
package org.projectnessie.gc.tool.cli.options;

import java.util.Arrays;
import java.util.Locale;
import java.util.stream.Stream;
import picocli.CommandLine.IDefaultValueProvider;
import picocli.CommandLine.Model.ArgSpec;
import picocli.CommandLine.Model.OptionSpec;

/**
 * Provides default values for command options.
 *
 * <p>Considers all "long" options (starting with {@code --}), using the conversion {@code
 * "NESSIE_GC_" + optionName..substring(2).replace('-', '_').toUpperCase()}.
 *
 * <p>For example, the default value for the command line option {@code --jdbc-passowrd} is derived
 * from the environment variable {@code NESSIE_GC_JDBC_PASSWORD}.
 */
public class EnvironmentDefaultProvider implements IDefaultValueProvider {

  @Override
  public String defaultValue(ArgSpec argSpec) {
    if (argSpec.isOption()) {
      OptionSpec optionSpec = (OptionSpec) argSpec;
      return Arrays.stream(optionSpec.names())
          .filter(name -> name.startsWith("--"))
          .map(name -> "NESSIE_GC_" + name.substring(2).replace('-', '_').toUpperCase(Locale.ROOT))
          .flatMap(
              name -> {
                String envVal = System.getenv(name);
                return envVal != null ? Stream.of(envVal) : Stream.empty();
              })
          .findFirst()
          .orElse(null);
    }
    return null;
  }
}
