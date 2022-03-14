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

import java.util.concurrent.Callable;
import javax.inject.Inject;
import org.projectnessie.quarkus.config.VersionStoreConfig;
import org.projectnessie.quarkus.config.VersionStoreConfig.VersionStoreType;
import org.projectnessie.services.config.ServerConfig;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Spec;

public abstract class BaseCommand implements Callable<Integer> {
  @Inject DatabaseAdapter databaseAdapter;
  @Inject VersionStoreConfig versionStoreConfig;
  @Inject ServerConfig serverConfig;

  @Spec CommandSpec spec;

  protected void warnOnInMemory() {
    if (versionStoreConfig.getVersionStoreType() == VersionStoreType.INMEMORY) {
      spec.commandLine()
          .getErr()
          .println(
              spec.commandLine()
                  .getColorScheme()
                  .errorText(
                      ""
                          + "****************************************************************************************\n"
                          + "** Repository information & maintenance for the INMEMORY implementation is meaningless\n"
                          + "****************************************************************************************\n"));
    }
  }
}
