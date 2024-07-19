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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.junit.main.QuarkusMainLauncher;
import io.quarkus.test.junit.main.QuarkusMainTest;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.catalog.service.objtypes.EntitySnapshotObj;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.IcebergView;
import org.projectnessie.nessie.tasks.api.TaskState;
import org.projectnessie.quarkus.tests.profiles.BaseConfigProfile;
import org.projectnessie.versioned.storage.common.exceptions.ObjNotFoundException;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.Persist;

@QuarkusMainTest
@TestProfile(BaseConfigProfile.class)
@ExtendWith(NessieServerAdminTestExtension.class)
class ITDeleteCatalogTasks extends AbstractContentTests<String> {

  ITDeleteCatalogTasks(Persist persist) {
    super(persist, String.class);
  }

  private ObjId storeNewEntry() {
    return storeNewEntry(TaskState.failureState("test", null));
  }

  private ObjId storeNewEntry(TaskState state) {
    String id = UUID.randomUUID().toString();
    IcebergTable table = IcebergTable.of("loc_" + id, 1, 2, 3, 4, id);
    return storeNewEntry(state, ContentKey.of("ns", "t_" + table.getId()), table);
  }

  private ObjId storeNewEntry(ContentKey key, Content content) {
    return storeNewEntry(TaskState.failureState("test", null), key, content);
  }

  private ObjId storeNewEntry(TaskState state, ContentKey key, Content content) {
    try {
      commit(content, key, true);
      ObjId id = EntitySnapshotObj.snapshotObjIdForContent(content);
      persist()
          .storeObj(EntitySnapshotObj.builder().id(id).taskState(state).versionToken("v1").build());
      return id;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testExpireAll(QuarkusMainLauncher launcher, Persist persist) {
    List<ObjId> ids =
        IntStream.iterate(0, n -> n < 123, n -> n + 1).mapToObj(i -> storeNewEntry()).toList();

    launchNoFile(launcher, "delete-catalog-tasks", "--batch", "11");

    assertThat(result.getOutputStream())
        .anyMatch(l -> l.contains("Deleted 11 snapshot task object(s)..."));
    assertThat(result.getOutputStream())
        .anyMatch(l -> l.contains("Deleted 123 snapshot task object(s) in total."));
    assertThat(result.exitCode()).isEqualTo(0);

    assertThat(ids)
        .allSatisfy(
            id ->
                assertThatThrownBy(
                        () ->
                            persist.fetchTypedObj(
                                id, EntitySnapshotObj.OBJ_TYPE, EntitySnapshotObj.class))
                    .isInstanceOf(ObjNotFoundException.class));
  }

  @Test
  public void testExpireByKey(QuarkusMainLauncher launcher, Persist persist)
      throws ObjNotFoundException {
    ObjId id1 =
        storeNewEntry(
            ContentKey.of("ns", "v1"), IcebergView.of(UUID.randomUUID().toString(), "loc1", 1, 2));
    ObjId id2 =
        storeNewEntry(
            ContentKey.of("ns", "v2"), IcebergView.of(UUID.randomUUID().toString(), "loc2", 1, 2));

    launchNoFile(launcher, "delete-catalog-tasks", "-k", "ns", "-k", "v2");

    assertThat(result.getOutputStream())
        .anyMatch(l -> l.contains("Deleted 1 snapshot task object(s)..."));
    assertThat(result.getOutputStream())
        .anyMatch(l -> l.contains("Deleted 1 snapshot task object(s) in total."));
    assertThat(result.exitCode()).isEqualTo(0);

    assertThatThrownBy(
            () -> persist.fetchTypedObj(id2, EntitySnapshotObj.OBJ_TYPE, EntitySnapshotObj.class))
        .isInstanceOf(ObjNotFoundException.class);

    assertThat(persist.fetchTypedObj(id1, EntitySnapshotObj.OBJ_TYPE, EntitySnapshotObj.class))
        .isNotNull();
  }

  @Test
  public void testExpireByStatus(QuarkusMainLauncher launcher, Persist persist)
      throws ObjNotFoundException {
    ObjId id1 = storeNewEntry(TaskState.SUCCESS);
    ObjId id2 = storeNewEntry(TaskState.failureState("test2", null));
    ObjId id3 =
        storeNewEntry(TaskState.retryableErrorState(Instant.ofEpochMilli(0), "test3", "test"));

    launchNoFile(
        launcher, "delete-catalog-tasks", "--task-status=SUCCESS", "--task-status=FAILURE");

    assertThat(result.getOutputStream())
        .anyMatch(l -> l.contains("Deleted 2 snapshot task object(s)..."));
    assertThat(result.getOutputStream())
        .anyMatch(l -> l.contains("Deleted 2 snapshot task object(s) in total."));
    assertThat(result.exitCode()).isEqualTo(0);

    assertThatThrownBy(
            () -> persist.fetchTypedObj(id1, EntitySnapshotObj.OBJ_TYPE, EntitySnapshotObj.class))
        .isInstanceOf(ObjNotFoundException.class);
    assertThatThrownBy(
            () -> persist.fetchTypedObj(id2, EntitySnapshotObj.OBJ_TYPE, EntitySnapshotObj.class))
        .isInstanceOf(ObjNotFoundException.class);

    assertThat(persist.fetchTypedObj(id3, EntitySnapshotObj.OBJ_TYPE, EntitySnapshotObj.class))
        .isNotNull();
  }
}
