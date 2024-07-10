/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.versioned.transfer.testing;

import java.util.Collections;
import java.util.Set;
import org.projectnessie.model.Content;
import org.projectnessie.versioned.storage.common.objtypes.CommitObj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.Reference;
import org.projectnessie.versioned.transfer.related.TransferRelatedObjects;

/**
 * {@link TransferRelatedObjects} implementation used by {@code
 * org.projectnessie.versioned.transfer.TestExportImportV3#relatedObjectsJarWorks(boolean,
 * java.lang.String, long, long, boolean)}.
 */
public class RelatedObjectsForTesting implements TransferRelatedObjects {
  @Override
  public Set<ObjId> repositoryRelatedObjects() {
    return Collections.singleton(ObjId.objIdFromString("deadbeef"));
  }

  @Override
  public Set<ObjId> commitRelatedObjects(CommitObj commitObj) {
    return Collections.singleton(ObjId.objIdFromString("cafebabe"));
  }

  @Override
  public Set<ObjId> contentRelatedObjects(Content content) {
    return Collections.singleton(ObjId.objIdFromString("f00dfeed"));
  }

  @Override
  public Set<ObjId> referenceRelatedObjects(Reference reference) {
    return Collections.singleton(ObjId.objIdFromString("1dea5eed"));
  }
}
