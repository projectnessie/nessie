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

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.MethodOrderer.Alphanumeric;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

@TestMethodOrder(Alphanumeric.class)
public class TestHadoopViews extends TestHadoopViewBase {
  int readVersionHint() throws IOException {
    return Integer.parseInt(Files.readFirstLine(versionHintFile, Charsets.UTF_8));
  }

  @Test
  public void testView1() throws Exception {
    viewDefinition = new BaseViewDefinition(DEF, SCHEMA, "", new ArrayList<>());
    Map<String, String> properties = new HashMap<>();
    properties.put(CommonViewConstants.ENGINE_VERSION, "test-engine");
    VIEWS.create(tableLocation, viewDefinition, properties);
    assertThat(tableDir).exists();
    assertThat(metadataDir).exists().isDirectory();
    assertThat(version(1)).exists().isFile();
    assertThat(version(2)).doesNotExist();
    assertThat(versionHintFile).exists();
    assertThat(readVersionHint()).isEqualTo(1);
  }

  @Test
  public void testView2() throws Exception {
    VIEWS.replace(tableLocation, viewDefinition, new HashMap<>());
    assertThat(tableDir).exists();
    assertThat(metadataDir).exists().isDirectory();
    assertThat(version(1)).exists().isFile();
    assertThat(version(2)).exists().isFile();
    assertThat(version(3)).doesNotExist();
    assertThat(versionHintFile).exists();
    assertThat(readVersionHint()).isEqualTo(2);
  }

  @Test
  public void testView3() throws Exception {
    VIEWS.replace(tableLocation, viewDefinition, new HashMap<>());
    assertThat(tableDir).exists();
    assertThat(metadataDir).exists().isDirectory();
    assertThat(version(1)).exists().isFile();
    assertThat(version(2)).exists().isFile();
    assertThat(version(3)).exists().isFile();
    assertThat(version(4)).doesNotExist();
    assertThat(versionHintFile).exists();
    assertThat(readVersionHint()).isEqualTo(3);
  }

  @Test
  public void testView4() throws Exception {
    View view = VIEWS.load(tableLocation);
    view.updateProperties().set("version.history.num-entries", "3").commit();
    view.updateProperties().set(ViewProperties.TABLE_COMMENT, "A dummy table comment").commit();
    VIEWS.replace(tableLocation, viewDefinition, new HashMap<>());
    assertThat(tableDir).exists();
    assertThat(metadataDir).exists().isDirectory();
    assertThat(version(1)).exists().isFile();
    assertThat(version(2)).exists().isFile();
    assertThat(version(3)).exists().isFile();
    assertThat(version(4)).exists().isFile();
    assertThat(version(5)).exists().isFile();
    assertThat(version(6)).exists().isFile();
    assertThat(version(7)).doesNotExist();
    assertThat(versionHintFile).exists();
    assertThat(readVersionHint()).isEqualTo(6);
  }

  @Test
  public void testView5() {
    VIEWS.drop(tableLocation);
    assertThat(metadataDir).doesNotExist();
    assertThat(tableDir).doesNotExist();
  }
}
