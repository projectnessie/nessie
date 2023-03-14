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

import java.util.ArrayList;
import java.util.HashMap;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestViewCreateReplace extends TestViewBase {

  @Test
  public void testViewCreateReplace() {
    ViewVersionMetadata viewVersionMetadata =
        TestViews.loadViewVersionMetadata(metadataDir, "test");
    ViewDefinition oldViewMetadata = viewVersionMetadata.definition();
    ViewDefinition viewMetadata =
        ViewDefinition.of(
            "select sum(1) from base_tab", oldViewMetadata.schema(), "", new ArrayList<>());
    TestViews.replace(metadataDir, "test", viewMetadata, new HashMap<>());

    // Change the view sql
    viewVersionMetadata = TestViews.loadViewVersionMetadata(metadataDir, "test");
    oldViewMetadata = viewVersionMetadata.definition();
    Assertions.assertEquals(oldViewMetadata.sql(), "select sum(1) from base_tab");

    // Change the schema, session catalog and engine version
    Schema newSchema =
        new Schema(
            required(1, "id", Types.IntegerType.get()),
            required(2, "intData", Types.IntegerType.get()));
    viewMetadata =
        ViewDefinition.of(
            "select id, intData from base_tab", newSchema, "new catalog", new ArrayList<>());
    TestViews.replace(metadataDir, "test", viewMetadata, new HashMap<>());

    // Assert that the replaced view has the correct changes
    viewVersionMetadata = TestViews.loadViewVersionMetadata(metadataDir, "test");
    oldViewMetadata = viewVersionMetadata.definition();
    Assertions.assertEquals(oldViewMetadata.schema().toString(), newSchema.toString());
    Assertions.assertEquals(oldViewMetadata.sessionCatalog(), "new catalog");
    Assertions.assertEquals(
        viewVersionMetadata
            .currentVersion()
            .summary()
            .properties()
            .get(CommonViewConstants.ENGINE_VERSION),
        "TestEngine");

    View view = TestViews.load(null, "test");
    view.updateProperties().set(ViewProperties.TABLE_COMMENT, "A dummy table comment").commit();

    viewVersionMetadata = TestViews.loadViewVersionMetadata(metadataDir, "test");

    // Expect to see the view comment
    Assertions.assertEquals(
        viewVersionMetadata.properties().get(ViewProperties.TABLE_COMMENT),
        "A dummy table comment");
    // Expect to see three versions
    Assertions.assertEquals(viewVersionMetadata.currentVersionId(), 3);
    Assertions.assertEquals(viewVersionMetadata.versions().size(), 3);
    Assertions.assertEquals(viewVersionMetadata.versions().get(2).parentId().longValue(), 2);
  }
}
