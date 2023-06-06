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
package org.projectnessie.versioned.transfer;

import static org.projectnessie.versioned.storage.common.objtypes.CommitHeaders.EMPTY_COMMIT_HEADERS;
import static org.projectnessie.versioned.storage.common.objtypes.CommitOp.COMMIT_OP_SERIALIZER;
import static org.projectnessie.versioned.storage.common.persist.ObjId.objIdFromString;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.List;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.ser.Views;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.storage.common.indexes.StoreIndexes;
import org.projectnessie.versioned.storage.common.objtypes.CommitObj;
import org.projectnessie.versioned.storage.common.persist.ObjId;

final class ExportImportTestUtil {
  private ExportImportTestUtil() {}

  static final ObjectMapper MAPPER = new ObjectMapper();

  static ObjId intToObjId(int i) {
    return objIdFromString(String.format("%08x", i));
  }

  static Hash intToHash(int i) {
    return Hash.of(String.format("%08x", i));
  }

  static ByteString commitMeta(int i) {
    try {
      return ByteString.copyFromUtf8(
          MAPPER
              .writerWithView(Views.V1.class)
              .writeValueAsString(CommitMeta.fromMessage("commit # " + i)));
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  static CommitObj toCommitObj(int i) {
    return toCommitObj(i, 0);
  }

  static final ByteString EMPTY_COMMIT_INDEX =
      StoreIndexes.emptyImmutableIndex(COMMIT_OP_SERIALIZER).serialize();

  static CommitObj toCommitObj(int branchAndCommit, int commitsBetweenBranches) {
    return CommitObj.commitBuilder()
        .id(intToObjId(branchAndCommit))
        .created(100L + branchAndCommit)
        .seq(commitSeq(branchAndCommit, commitsBetweenBranches))
        .addTail(parentCommitId(branchAndCommit, commitsBetweenBranches))
        .message("commit # " + branchAndCommit)
        .headers(EMPTY_COMMIT_HEADERS)
        .incrementalIndex(EMPTY_COMMIT_INDEX)
        .build();
  }

  static long commitSeq(int branchAndCommit, int commitsBetweenBranches) {
    int branch = branchAndCommit >> 16;
    if (branch == 0 || commitsBetweenBranches == 0) {
      return branchAndCommit;
    }
    int num = branchAndCommit & 0xffff;
    int off = commitsBetweenBranches * branch;
    return 1L + off + num;
  }

  static ObjId parentCommitId(int branchAndCommit, int commitsBetweenBranches) {
    int branch = branchAndCommit >> 16;
    if (branch > 0 && commitsBetweenBranches > 0) {
      int num = branchAndCommit & 0xffff;
      if (num == 0) {
        return intToObjId(branch * commitsBetweenBranches);
      }
    }
    return intToObjId(branchAndCommit - 1);
  }

  static Hash parentCommitHash(int branchAndCommit, int commitsBetweenBranches) {
    int branch = branchAndCommit >> 16;
    if (branch > 0 && commitsBetweenBranches > 0) {
      int num = branchAndCommit & 0xffff;
      if (num == 0) {
        return intToHash(branch * commitsBetweenBranches);
      }
    }
    return intToHash(branchAndCommit - 1);
  }

  static List<Hash> expectedParents(int i, int commitsBetweenBranches, int parentsPerCommit) {
    List<Hash> parents = new ArrayList<>();
    while (true) {
      Hash p = parentCommitHash(i, commitsBetweenBranches);

      String hex = p.asString();

      parents.add(p);
      if ("ffffffff".equals(hex)) {
        break;
      }

      if (parents.size() == parentsPerCommit) {
        break;
      }
      i = Integer.parseInt(hex, 16);
    }
    return parents;
  }
}
