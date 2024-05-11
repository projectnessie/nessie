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
package org.projectnessie.tools.admin.cli;

import java.io.InputStream;
import java.util.Properties;
import picocli.CommandLine.IVersionProvider;

public class NessieVersionProvider implements IVersionProvider {
  @Override
  public String[] getVersion() throws Exception {
    try (InputStream input =
        NessieVersionProvider.class
            .getResource("version.properties")
            .openConnection()
            .getInputStream()) {
      Properties props = new Properties();
      props.load(input);
      return new String[] {props.getProperty("nessie.version")};
    }
  }
}
