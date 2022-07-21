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
package org.projectnessie.services.impl;

import java.security.Principal;
import java.util.List;
import java.util.stream.Collectors;
import org.projectnessie.api.UnreachableHeadsApi;
import org.projectnessie.error.NessieReferenceNotFoundException;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.model.ImmutableUnreachableHeadsResponse;
import org.projectnessie.model.UnreachableHeadsResponse;
import org.projectnessie.services.authz.Authorizer;
import org.projectnessie.services.config.ServerConfig;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.GetNamedRefsParams;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.ImmutableGetNamedRefsParams;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.VersionStore;

public class UnreachableHeadsApiImpl extends BaseApiImpl implements UnreachableHeadsApi {

  public UnreachableHeadsApiImpl(
      ServerConfig config,
      VersionStore<Content, CommitMeta, Content.Type> store,
      Authorizer authorizer,
      Principal principal) {
    super(config, store, authorizer, principal);
  }

  @Override
  public UnreachableHeadsResponse getUnreachableReferenceHeads()
      throws NessieReferenceNotFoundException {
    ImmutableGetNamedRefsParams namedRefsParams =
        GetNamedRefsParams.builder()
            .baseReference(BranchName.of(this.getConfig().getDefaultBranch()))
            .branchRetrieveOptions(
                GetNamedRefsParams.RetrieveOptions.BASE_REFERENCE_RELATED_AND_COMMIT_META)
            .tagRetrieveOptions(GetNamedRefsParams.RetrieveOptions.COMMIT_META)
            .build();
    try {
      long totalCommitsOnDefaultBranch =
          getStore().getNamedRef(getConfig().getDefaultBranch(), namedRefsParams).getCommitSeq();
      List<String> entries =
          getStore().getUnreachableHeads(totalCommitsOnDefaultBranch).stream()
              .map(Hash::asString)
              .collect(Collectors.toList());
      return ImmutableUnreachableHeadsResponse.builder().addAllEntries(entries).build();
    } catch (ReferenceNotFoundException e) {
      throw new NessieReferenceNotFoundException(e.getMessage(), e);
    }
  }
}
