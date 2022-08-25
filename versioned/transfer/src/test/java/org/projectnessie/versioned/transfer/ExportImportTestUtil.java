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

import static java.util.Collections.emptyList;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.persist.adapter.CommitLogEntry;

final class ExportImportTestUtil {
  private ExportImportTestUtil() {}

  static final ObjectMapper MAPPER = new ObjectMapper();

  static Hash intToHash(int i) {
    return Hash.of(String.format("%08x", i));
  }

  static ByteString commitMeta(int i) {
    try {
      return ByteString.copyFromUtf8(
          MAPPER.writeValueAsString(CommitMeta.fromMessage("commit # " + i)));
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  static CommitLogEntry toCommitLogEntry(int i) {
    return toCommitLogEntry(i, 0);
  }

  static CommitLogEntry toCommitLogEntry(int branchAndCommit, int commitsBetweenBranches) {
    return CommitLogEntry.of(
        100L + branchAndCommit,
        intToHash(branchAndCommit),
        commitSeq(branchAndCommit, commitsBetweenBranches),
        Collections.singletonList(parentCommitHash(branchAndCommit, commitsBetweenBranches)),
        commitMeta(branchAndCommit),
        emptyList(),
        emptyList(),
        Integer.MAX_VALUE - 1,
        null,
        emptyList(),
        emptyList(),
        emptyList());
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
