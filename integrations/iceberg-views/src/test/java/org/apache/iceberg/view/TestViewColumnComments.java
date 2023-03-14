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

import org.apache.iceberg.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestViewColumnComments extends TestViewBase {

  @Test
  public void testColumnCommentsUpdate() {
    TestViews.TestViewOperations ops = new TestViews.TestViewOperations("test", metadataDir);
    CommentUpdate commentUpdate = new CommentUpdate(ops);
    commentUpdate.updateColumnDoc("c1", "The column name is c1");
    Schema schema = commentUpdate.apply();
    Assertions.assertEquals(schema.findField("c1").doc(), "The column name is c1");

    commentUpdate.updateColumnDoc("c1", "The column name is c1 and type is integer");
    schema = commentUpdate.apply();
    Assertions.assertEquals(
        schema.findField("c1").doc(), "The column name is c1 and type is integer");
    commentUpdate.commit();

    commentUpdate.updateColumnDoc("c2", "The column name is c2 and type is integer");
    schema = commentUpdate.apply();
    Assertions.assertEquals(
        schema.findField("c2").doc(), "The column name is c2 and type is integer");

    commentUpdate.updateColumnDoc("c2", "The column name is c2 and type is string");
    commentUpdate.updateColumnDoc("c2", "");
    schema = commentUpdate.apply();
    Assertions.assertEquals(schema.findField("c2").doc(), "");
  }
}
