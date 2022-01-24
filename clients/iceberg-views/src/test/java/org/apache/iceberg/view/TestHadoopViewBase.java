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
package org.apache.iceberg.view;

import static org.apache.iceberg.types.Types.NestedField.required;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import java.io.File;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;

public class TestHadoopViewBase {

  // Schema passed to create tables
  static final Schema SCHEMA =
      new Schema(
          required(3, "c1", Types.IntegerType.get(), "unique ID"),
          required(4, "c2", Types.IntegerType.get()));
  static final String DEF = "select * from base";

  static final HadoopViews VIEWS = new HadoopViews(new Configuration());

  static File tableDir = null;
  static String tableLocation = null;
  static File metadataDir = null;
  static File versionHintFile = null;
  static ViewDefinition viewDefinition = null;

  @BeforeAll
  public static void setupTable(@TempDir File dir) {
    tableDir = dir;
    tableLocation = tableDir.toURI().toString();
    metadataDir = new File(tableDir, "metadata");
    versionHintFile = new File(metadataDir, "version-hint.text");
  }

  File version(int i) {
    return new File(metadataDir, "v" + i + ".json");
  }

  int readVersionHint() throws IOException {
    return Integer.parseInt(Files.readFirstLine(versionHintFile, Charsets.UTF_8));
  }
}
