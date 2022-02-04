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

import com.google.common.collect.Iterators;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

public class TestViewBase {
  // Schema passed to create tables
  public static final Schema SCHEMA =
      new Schema(
          required(3, "c1", Types.IntegerType.get()), required(4, "c2", Types.StringType.get()));

  public static final String SQL = "select c1, c2 from base_tab";

  File tableDir = null;
  File metadataDir = null;

  @BeforeEach
  public void setupTable(@TempDir File dir) throws Exception {
    this.tableDir = dir;
    this.metadataDir = new File(tableDir, "metadata");
    create(SCHEMA);
  }

  @AfterEach
  public void cleanupTables() {
    TestViews.drop("test", metadataDir);
    TestViews.clearTables();
  }

  private BaseView create(Schema schema) {
    ViewDefinition viewMetadata = ViewDefinition.of(SQL, SCHEMA, "", new ArrayList<>());
    TestViews.create(tableDir, "test", viewMetadata, new HashMap<>());
    return TestViews.load(tableDir, "test");
  }

  BaseView load() {
    return TestViews.load(tableDir, "test");
  }

  Integer version() {
    return TestViews.metadataVersion("test");
  }

  static Iterator<Long> ids(Long... ids) {
    return Iterators.forArray(ids);
  }

  static Iterator<DataFile> files(DataFile... files) {
    return Iterators.forArray(files);
  }

  static Iterator<DataFile> files(ManifestFile manifest) {
    return null;
    // return ManifestReader.read(localInput(manifest.path())).iterator();
  }
}
