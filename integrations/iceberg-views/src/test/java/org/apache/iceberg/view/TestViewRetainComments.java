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

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

import java.util.ArrayList;
import java.util.HashMap;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestViewRetainComments extends TestViewBase {

  @Test
  public void testViewRetainComments() {
    // Set some column comments
    Schema schema =
        new Schema(
            required(3, "c1", Types.IntegerType.get(), "c1 comment"),
            required(4, "c2", Types.StringType.get(), "c2 comment"));
    ViewDefinition viewMetadata =
        ViewDefinition.of("select sum(1) from base_tab", schema, "", new ArrayList<>());
    TestViews.replace(metadataDir, "test", viewMetadata, new HashMap<>());

    // Assert that the replaced view has the correct changes
    ViewVersionMetadata viewVersionMetadata =
        TestViews.loadViewVersionMetadata(metadataDir, "test");
    ViewDefinition oldViewMetadata = viewVersionMetadata.definition();
    Assertions.assertEquals(oldViewMetadata.schema().toString(), schema.toString());

    // Change the schema, change the name and data type of one of the columns. Column comments are
    // not expected to persist for that column (because of name change). Change the data type of the
    // other
    // column. Column comments are supposed to persist.
    Schema newSchema =
        new Schema(
            required(1, "c1", Types.StringType.get()),
            required(2, "intData", Types.IntegerType.get()));
    viewMetadata =
        ViewDefinition.of(
            "select c1, intData from base_tab", newSchema, "new catalog", new ArrayList<>());
    TestViews.replace(metadataDir, "test", viewMetadata, new HashMap<>());

    // Assert that the replaced view has the correct changes
    viewVersionMetadata = TestViews.loadViewVersionMetadata(metadataDir, "test");
    oldViewMetadata = viewVersionMetadata.definition();
    Assertions.assertEquals(oldViewMetadata.schema().columns().get(0).doc(), "c1 comment");
    Assertions.assertEquals(oldViewMetadata.schema().columns().get(1).doc(), null);
    Assertions.assertEquals(oldViewMetadata.sessionCatalog(), "new catalog");
    Assertions.assertEquals(
        viewVersionMetadata
            .currentVersion()
            .summary()
            .properties()
            .get(CommonViewConstants.ENGINE_VERSION),
        "TestEngine");

    // Reset by setting some column comments
    schema =
        new Schema(
            required(3, "c1", Types.IntegerType.get(), "c1 comment"),
            required(4, "intData", Types.StringType.get(), "c2 comment"));
    viewMetadata = ViewDefinition.of("select sum(1) from base_tab", schema, "", new ArrayList<>());
    TestViews.replace(metadataDir, "test", viewMetadata, new HashMap<>());

    // Change the required/optional attribute and data type of a column, column comment is expected
    // to persist.
    // Null out the column comment by setting the comment to empty string.
    newSchema =
        new Schema(
            optional(1, "c1", Types.IntegerType.get()),
            required(2, "intData", Types.IntegerType.get(), ""));
    viewMetadata =
        ViewDefinition.of(
            "select c1, intData from base_tab", newSchema, "new catalog", new ArrayList<>());
    TestViews.replace(metadataDir, "test", viewMetadata, new HashMap<>());

    // Assert that the replaced view has the correct changes
    viewVersionMetadata = TestViews.loadViewVersionMetadata(metadataDir, "test");
    oldViewMetadata = viewVersionMetadata.definition();
    Assertions.assertEquals(oldViewMetadata.schema().columns().get(0).doc(), "c1 comment");
    Assertions.assertEquals(oldViewMetadata.schema().columns().get(0).isOptional(), true);
    Assertions.assertEquals(oldViewMetadata.schema().columns().get(1).doc(), "");
  }
}
