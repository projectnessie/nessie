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

import static org.projectnessie.quarkus.config.VersionStoreConfig.VersionStoreType.IN_MEMORY;

import jakarta.enterprise.inject.Default;
import jakarta.inject.Inject;
import java.util.concurrent.Callable;
import org.projectnessie.quarkus.config.VersionStoreConfig;
import org.projectnessie.services.config.ServerConfig;
import org.projectnessie.versioned.storage.common.persist.Persist;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Spec;

public abstract class BaseCommand implements Callable<Integer> {
  @Inject @Default Persist persist;

  @Inject VersionStoreConfig versionStoreConfig;
  @Inject ServerConfig serverConfig;

  @Spec CommandSpec spec;

  public static final Integer EXIT_CODE_GENERIC_ERROR = 1;
  public static final Integer EXIT_CODE_CONTENT_ERROR = 3;
  public static final Integer EXIT_CODE_REPO_DOES_NOT_EXIST = 4;
  public static final Integer EXIT_CODE_REPO_ALREADY_EXISTS = 100;

  public abstract Integer call() throws Exception;

  protected void warnOnInMemory() {
    if (versionStoreConfig.getVersionStoreType() == IN_MEMORY) {
      spec.commandLine()
          .getErr()
          .println(
              spec.commandLine()
                  .getColorScheme()
                  .errorText(
                      "****************************************************************************************\n"
                          + "** Repository information & maintenance for an in-memory implementation is meaningless\n"
                          + "****************************************************************************************\n"));
    }
  }
}
