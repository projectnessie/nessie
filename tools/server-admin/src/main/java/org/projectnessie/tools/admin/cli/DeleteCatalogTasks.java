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
package org.projectnessie.tools.admin.cli;

import static org.projectnessie.versioned.storage.common.logic.Logics.commitLogic;
import static org.projectnessie.versioned.storage.common.logic.Logics.indexesLogic;
import static org.projectnessie.versioned.storage.versionstore.RefMapping.hashNotFound;
import static org.projectnessie.versioned.storage.versionstore.TypeMapping.hashToObjId;
import static org.projectnessie.versioned.storage.versionstore.TypeMapping.storeKeyToKey;

import com.google.common.collect.Iterators;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Spliterators;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.projectnessie.catalog.service.objtypes.EntitySnapshotObj;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.IcebergView;
import org.projectnessie.nessie.tasks.api.TaskStatus;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.storage.common.exceptions.ObjNotFoundException;
import org.projectnessie.versioned.storage.common.indexes.StoreIndex;
import org.projectnessie.versioned.storage.common.objtypes.CommitObj;
import org.projectnessie.versioned.storage.common.objtypes.CommitOp;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.versionstore.ContentMapping;
import picocli.CommandLine;

@CommandLine.Command(
    name = "delete-catalog-tasks",
    mixinStandardHelpOptions = true,
    description =
        "Delete persisted state of Iceberg snapshot loading tasks previously executed by the Nessie Catalog.")
public class DeleteCatalogTasks extends BaseCommand {

  @CommandLine.Option(
      names = {"-B", "--batch"},
      defaultValue = "100",
      description = {"The max number of task IDs to process at the same time."})
  private int batchSize;

  @CommandLine.Option(
      names = {"-k", "--key-element"},
      description =
          "Elements or a specific content key to process (zero or more). "
              + "If not set, all current keys will get their snapshot tasks expired.")
  private List<String> keyElements;

  @CommandLine.Option(
      names = {"-s", "--task-status"},
      defaultValue = "FAILURE",
      description =
          "Delete tasks having these statuses (zero or more). "
              + "If not set, only failed tasks for matching content objects are deleted.")
  private EnumSet<TaskStatus> statuses;

  @CommandLine.Option(
      names = {"-r", "--ref"},
      description = "Reference name to use (default branch, if not set).")
  private String ref;

  @CommandLine.Option(
      names = {"-H", "--hash"},
      description = "Commit hash to use (defaults to the HEAD of the specified reference).")
  private String hash;

  private final AtomicInteger idsProcessed = new AtomicInteger();

  @Override
  public Integer call() throws ReferenceNotFoundException {
    warnOnInMemory();

    StoreIndex<CommitOp> index = index(hash(hash, ref));
    try (Stream<ContentKey> keys = iterateKeys(index)) {
      Iterators.partition(keys.iterator(), batchSize)
          .forEachRemaining(
              objects -> {
                Map<ContentKey, Content> values = fetchValues(index, objects);
                ObjId[] ids =
                    values.values().stream()
                        .filter(c -> c instanceof IcebergTable || c instanceof IcebergView)
                        .map(EntitySnapshotObj::snapshotObjIdForContent)
                        .toArray(ObjId[]::new);

                EntitySnapshotObj[] objs =
                    persist.fetchTypedObjsIfExist(
                        ids, EntitySnapshotObj.OBJ_TYPE, EntitySnapshotObj.class);

                ObjId[] idsToDelete =
                    Arrays.stream(objs)
                        .filter(Objects::nonNull)
                        .filter(o -> statuses.contains(o.taskState().status()))
                        .map(Obj::id)
                        .toArray(ObjId[]::new);

                persist.deleteObjs(idsToDelete);
                idsProcessed.addAndGet(idsToDelete.length);
                spec.commandLine()
                    .getOut()
                    .printf("Deleted %d snapshot task object(s)...%n", idsToDelete.length);
              });
    }

    spec.commandLine()
        .getOut()
        .printf(
            "Deleted %d snapshot task object(s) in total.%n%n"
                + "Note: Catalogs Server likely need to be restarted to reload correct snapshot metadata.%n",
            idsProcessed.get());
    return 0;
  }

  private Map<ContentKey, Content> fetchValues(StoreIndex<CommitOp> index, List<ContentKey> keys) {
    ContentMapping contentMapping = new ContentMapping(persist);
    try {
      return contentMapping.fetchContents(index, keys);
    } catch (ObjNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  protected Stream<ContentKey> iterateKeys(StoreIndex<CommitOp> index) {
    if (keyElements != null && !keyElements.isEmpty()) {
      return Stream.of(ContentKey.of(keyElements));
    }

    return StreamSupport.stream(
            Spliterators.spliterator(index.iterator(null, null, true), 0, 0), false)
        .map(
            indexElement -> {
              if (indexElement.content().action().exists()) {
                return storeKeyToKey(indexElement.key());
              } else {
                return null;
              }
            })
        .filter(Objects::nonNull);
  }

  private StoreIndex<CommitOp> index(Hash hash) throws ReferenceNotFoundException {
    try {
      CommitObj c = commitLogic(persist).fetchCommit(hashToObjId(hash));
      return indexesLogic(persist).buildCompleteIndexOrEmpty(c);
    } catch (ObjNotFoundException e) {
      throw hashNotFound(hash);
    }
  }
}
