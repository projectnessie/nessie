/*
 * Copyright (C) 2022 Dremio
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
package org.projectnessie.gc.iceberg;

import org.projectnessie.gc.contents.ContentReference;
import org.projectnessie.gc.identify.ContentToContentReference;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;

public final class IcebergContentToContentReference implements ContentToContentReference {

  public static final IcebergContentToContentReference INSTANCE =
      new IcebergContentToContentReference();

  private IcebergContentToContentReference() {}

  @Override
  public ContentReference contentToReference(Content content, String commitId, ContentKey key) {
    if (!(content instanceof IcebergTable)) {
      throw new IllegalArgumentException("Expect ICEBERG_TABLE, but got " + content.getType());
    }
    IcebergTable table = (IcebergTable) content;

    return ContentReference.icebergTable(
        content.getId(), commitId, key, table.getMetadataLocation(), table.getSnapshotId());
  }
}
