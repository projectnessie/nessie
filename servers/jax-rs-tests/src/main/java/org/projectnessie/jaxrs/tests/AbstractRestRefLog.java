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
package org.projectnessie.jaxrs.tests;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.OptionalInt;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import org.assertj.core.groups.Tuple;
import org.junit.jupiter.api.Test;
import org.projectnessie.client.StreamingUtil;
import org.projectnessie.client.ext.NessieApiVersion;
import org.projectnessie.client.ext.NessieApiVersions;
import org.projectnessie.error.BaseNessieClientServerException;
import org.projectnessie.error.NessieRefLogNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.RefLogResponse;
import org.projectnessie.model.Tag;

/** See {@link AbstractTestRest} for details about and reason for the inheritance model. */
@SuppressWarnings("deprecation")
public abstract class AbstractRestRefLog extends AbstractRestReferences {
  @Test
  @NessieApiVersions(versions = NessieApiVersion.V1)
  public void testReflog() throws BaseNessieClientServerException {

    String tagName = "tag1_test_reflog_" + ThreadLocalRandom.current().nextInt();
    String branch1 = "branch1_test_reflog";
    String branch2 = "branch2_test_reflog";
    String branch3 = "branch3_test_reflog";
    String root = "ref_name_test_reflog";

    List<Tuple> expectedEntries = new ArrayList<>();

    // reflog 1: creating the default branch0
    Branch branch0 = createBranch(root);
    expectedEntries.add(Tuple.tuple(root, "CREATE_REFERENCE"));

    // reflog 2: create tag1
    getApi()
        .createReference()
        .sourceRefName(branch0.getName())
        .reference(Tag.of(tagName, branch0.getHash()))
        .create();
    expectedEntries.add(Tuple.tuple(tagName, "CREATE_REFERENCE"));

    // reflog 3: create branch1
    getApi()
        .createReference()
        .sourceRefName(branch0.getName())
        .reference(Branch.of(branch1, branch0.getHash()))
        .create();
    expectedEntries.add(Tuple.tuple(branch1, "CREATE_REFERENCE"));

    // reflog 4: create branch2
    getApi()
        .createReference()
        .sourceRefName(branch0.getName())
        .reference(Branch.of(branch2, branch0.getHash()))
        .create();
    expectedEntries.add(Tuple.tuple(branch2, "CREATE_REFERENCE"));

    // reflog 5: create branch2
    getApi()
        .createReference()
        .sourceRefName(branch0.getName())
        .reference(Branch.of(branch3, branch0.getHash()))
        .create();
    expectedEntries.add(Tuple.tuple(branch3, "CREATE_REFERENCE"));

    // reflog 11: delete branch
    getApi().deleteBranch().branchName(branch1).hash(branch0.getHash()).delete();
    expectedEntries.add(Tuple.tuple(branch1, "DELETE_REFERENCE"));

    // reflog 12: delete tag
    getApi().deleteTag().tagName(tagName).hash(branch0.getHash()).delete();
    expectedEntries.add(Tuple.tuple(tagName, "DELETE_REFERENCE"));

    // In the reflog output new entry will be the head. Hence, reverse the expected list
    Collections.reverse(expectedEntries);

    RefLogResponse refLogResponse = getApi().getRefLog().get();
    // verify reflog entries
    soft.assertThat(refLogResponse.getLogEntries().subList(0, 7))
        .extracting(
            RefLogResponse.RefLogResponseEntry::getRefName,
            RefLogResponse.RefLogResponseEntry::getOperation)
        .isEqualTo(expectedEntries);
    // verify pagination (limit and token)
    RefLogResponse refLogResponse1 = getApi().getRefLog().maxRecords(2).get();
    soft.assertThat(refLogResponse1.getLogEntries())
        .isEqualTo(refLogResponse.getLogEntries().subList(0, 2));
    soft.assertThat(refLogResponse1.isHasMore()).isTrue();
    RefLogResponse refLogResponse2 =
        getApi().getRefLog().pageToken(refLogResponse1.getToken()).get();
    // should start from the token.
    soft.assertThat(refLogResponse2.getLogEntries().get(0).getRefLogId())
        .isEqualTo(refLogResponse1.getToken());
    soft.assertThat(refLogResponse2.getLogEntries().subList(0, 5))
        .isEqualTo(refLogResponse.getLogEntries().subList(2, 7));
    // verify startHash and endHash
    RefLogResponse refLogResponse3 =
        getApi().getRefLog().fromHash(refLogResponse.getLogEntries().get(4).getRefLogId()).get();
    soft.assertThat(refLogResponse3.getLogEntries().subList(0, 2))
        .isEqualTo(refLogResponse.getLogEntries().subList(4, 6));
    RefLogResponse refLogResponse4 =
        getApi()
            .getRefLog()
            .fromHash(refLogResponse.getLogEntries().get(3).getRefLogId())
            .untilHash(refLogResponse.getLogEntries().get(5).getRefLogId())
            .get();
    soft.assertThat(refLogResponse4.getLogEntries())
        .isEqualTo(refLogResponse.getLogEntries().subList(3, 6));

    // use invalid reflog id f1234d75178d892a133a410355a5a990cf75d2f33eba25d575943d4df632f3a4
    // computed using Hash.of(
    //    UnsafeByteOperations.unsafeWrap(newHasher().putString("invalid",
    // StandardCharsets.UTF_8).hash().asBytes()));
    soft.assertThatThrownBy(
            () ->
                getApi()
                    .getRefLog()
                    .fromHash("f1234d75178d892a133a410355a5a990cf75d2f33eba25d575943d4df632f3a4")
                    .get())
        .isInstanceOf(NessieRefLogNotFoundException.class)
        .hasMessageContaining(
            "RefLog entry for 'f1234d75178d892a133a410355a5a990cf75d2f33eba25d575943d4df632f3a4' does not exist");
    // test filter with stream
    List<RefLogResponse.RefLogResponseEntry> filteredResult =
        StreamingUtil.getReflogStream(
                getApi(),
                builder ->
                    builder.filter(
                        "reflog.operation == 'CREATE_REFERENCE' "
                            + "&& reflog.refName == '"
                            + tagName
                            + "'"),
                OptionalInt.empty())
            .collect(Collectors.toList());
    soft.assertThat(filteredResult.size()).isEqualTo(1);
    soft.assertThat(filteredResult.get(0))
        .extracting(
            RefLogResponse.RefLogResponseEntry::getRefName,
            RefLogResponse.RefLogResponseEntry::getOperation)
        .isEqualTo(expectedEntries.get(5).toList());
  }
}
