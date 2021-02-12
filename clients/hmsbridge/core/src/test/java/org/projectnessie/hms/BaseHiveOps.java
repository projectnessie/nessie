/*
 * Copyright (C) 2020 Dremio
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
package org.projectnessie.hms;

import java.util.function.Function;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.client.NessieClient;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.Reference;

import com.klarna.hiverunner.HiveRunnerExtension;
import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.annotations.HiveSQL;

@ExtendWith(HiveRunnerExtension.class)
public abstract class BaseHiveOps {

  private static final int NESSIE_PORT = Integer.getInteger("quarkus.http.test-port", 19121);
  protected static final String URL = String.format("http://localhost:%d/api/v1", NESSIE_PORT);

  protected static NessieClient client;

  @HiveSQL(files = {}, autoStart = false)
  protected HiveShell shell;

  @AfterAll
  static void shutdownClient() {
    if (client != null) {
      client.close();
    }
  }

  protected abstract Function<String, String> configFunction();

  @BeforeEach
  void resetData() throws NessieConflictException, NessieNotFoundException {
    NessieClient client = NessieClient.withConfig(configFunction());
    for (Reference r : client.getTreeApi().getAllReferences()) {
      if (r instanceof Branch) {
        client.getTreeApi().deleteBranch(r.getName(), r.getHash());
      } else {
        client.getTreeApi().deleteTag(r.getName(), r.getHash());
      }
    }
    client.getTreeApi().createReference(Branch.of("main", null));
    shell.start();
  }

}
