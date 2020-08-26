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
package com.dremio.nessie.serverless;

import com.dremio.nessie.error.NessieError;
import com.dremio.nessie.model.Branch;
import com.dremio.nessie.model.NessieConfiguration;
import com.dremio.nessie.model.Table;
import com.dremio.nessie.model.User;
import com.google.common.base.Joiner;

import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.builditem.nativeimage.ReflectiveClassBuildItem;

public class NessieModelProcessor {

  private static final Joiner DOT = Joiner.on(".");

  @BuildStep
  ReflectiveClassBuildItem reflection() {
    Class[] allClasss = new Class[]{
      Table.class,
      Branch.class,
      User.class,
      NessieConfiguration.class,
      NessieError.class
    };
    return new ReflectiveClassBuildItem(true, true, true, allClasss);
  }


}
