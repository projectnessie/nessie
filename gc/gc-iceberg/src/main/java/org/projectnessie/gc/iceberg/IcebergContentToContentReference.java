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

import static com.google.common.base.Preconditions.checkArgument;
import static org.projectnessie.gc.contents.ContentReference.icebergContent;
import static org.projectnessie.model.Content.Type.ICEBERG_TABLE;
import static org.projectnessie.model.Content.Type.ICEBERG_VIEW;

import org.projectnessie.gc.contents.ContentReference;
import org.projectnessie.gc.identify.ContentToContentReference;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergContent;

public final class IcebergContentToContentReference implements ContentToContentReference {

  public static final IcebergContentToContentReference INSTANCE =
      new IcebergContentToContentReference();

  private IcebergContentToContentReference() {}

  @Override
  public ContentReference contentToReference(Content content, String commitId, ContentKey key) {
    Content.Type contentType = content.getType();
    if (contentType.equals(ICEBERG_TABLE) || contentType.equals(ICEBERG_VIEW)) {
      checkArgument(
          contentType.type().isInstance(content),
          "Expect %s, but got %s",
          contentType.type().getSimpleName(),
          content.getClass().getName());
      IcebergContent icebergContent = (IcebergContent) content;

      return icebergContent(
          contentType,
          content.getId(),
          commitId,
          key,
          icebergContent.getMetadataLocation(),
          icebergContent.getVersionId());
    } else {
      throw new IllegalArgumentException(
          "Expect ICEBERG_TABLE/ICEBERG_VIEW, but got " + contentType);
    }
  }
}
