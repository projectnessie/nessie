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
package com.dremio.nessie.hms;

import com.dremio.nessie.client.NessieClient;
import com.google.common.collect.ImmutableMap;
import com.klarna.hiverunner.annotations.HiveProperties;
import java.util.Map;
import java.util.function.Function;
import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.jupiter.api.BeforeAll;
import org.projectnessie.DelegatingHive2NessieRawStore;

public class ITTestHive2DelegateOps extends BaseDelegateOps {

  @HiveProperties
  public static final Map<String, String> properties =
      ImmutableMap.<String, String>builder()
          .put(
              HiveConf.ConfVars.METASTORE_RAW_STORE_IMPL.varname,
              DelegatingHive2NessieRawStore.class.getName())
          .put("hive.exec.dynamic.partition.mode", "nonstrict")
          .put(NessieStore.NESSIE_WHITELIST_DBS_OPTION, "nessie,mytestdb")
          .put(NessieClient.CONF_NESSIE_URL, URL)
          .build();

  @BeforeAll
  static void setupNessieClient() {
    client = NessieClient.withConfig(properties::get);
  }

  @Override
  protected Function<String, String> configFunction() {
    return properties::get;
  }
}
