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

import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_URL;

import java.util.Map;
import java.util.function.Function;

import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.jupiter.api.BeforeAll;
import org.projectnessie.Hive2NessieRawStore;
import org.projectnessie.client.NessieClient;
import org.projectnessie.hms.BaseTableOperations;

import com.google.common.collect.ImmutableMap;
import com.klarna.hiverunner.annotations.HiveProperties;

public class ITTestHive2TableOperations extends BaseTableOperations {

  @HiveProperties
  public static final Map<String, String> properties = ImmutableMap.<String, String>builder()
      .put(HiveConf.ConfVars.METASTORE_RAW_STORE_IMPL.varname, Hive2NessieRawStore.class.getName())
      .put("hive.exec.dynamic.partition.mode","nonstrict")
      .put(CONF_NESSIE_URL, URL)
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
