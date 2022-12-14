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
package org.projectnessie.services.spi;

import java.util.Collection;
import java.util.List;
import javax.annotation.Nullable;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.EntriesResponse;
import org.projectnessie.model.FetchOption;
import org.projectnessie.model.LogResponse;
import org.projectnessie.model.MergeBehavior;
import org.projectnessie.model.MergeKeyBehavior;
import org.projectnessie.model.MergeResponse;
import org.projectnessie.model.Operations;
import org.projectnessie.model.Reference;
import org.projectnessie.model.ReferencesResponse;

/**
 * Server-side interface to services managing the content trees.
 *
 * <p>Refer to the javadoc of corresponding client-facing interfaces in the {@code model} module for
 * the meaning of various methods and their parameters.
 */
public interface TreeService {

  Branch getDefaultBranch() throws NessieNotFoundException;

  ReferencesResponse getAllReferences(FetchOption fetchOption, String filter);

  Reference getReferenceByName(String refName, FetchOption fetchOption)
      throws NessieNotFoundException;

  Reference createReference(
      String refName, Reference.ReferenceType type, String hash, String sourceRefName)
      throws NessieNotFoundException, NessieConflictException;

  Reference assignReference(
      Reference.ReferenceType referenceType,
      String referenceName,
      String expectedHash,
      Reference assignTo)
      throws NessieNotFoundException, NessieConflictException;

  void deleteReference(
      Reference.ReferenceType referenceType, String referenceName, String expectedHash)
      throws NessieConflictException, NessieNotFoundException;

  LogResponse getCommitLog(
      String namedRef,
      FetchOption fetchOption,
      String oldestHashLimit,
      String youngestHash,
      String filter,
      Integer maxRecords,
      String pageToken)
      throws NessieNotFoundException;

  MergeResponse transplantCommitsIntoBranch(
      String branchName,
      String expectedHash,
      String message,
      List<String> hashesToTransplant,
      String fromRefName,
      Boolean keepIndividualCommits,
      Collection<MergeKeyBehavior> keyMergeTypes,
      MergeBehavior defaultMergeType,
      Boolean dryRun,
      Boolean fetchAdditionalInfo,
      Boolean returnConflictAsResult)
      throws NessieNotFoundException, NessieConflictException;

  MergeResponse mergeRefIntoBranch(
      String branchName,
      String expectedHash,
      String fromRefName,
      String fromHash,
      Boolean keepIndividualCommits,
      @Nullable String message,
      Collection<MergeKeyBehavior> keyMergeTypes,
      MergeBehavior defaultMergeType,
      Boolean dryRun,
      Boolean fetchAdditionalInfo,
      Boolean returnConflictAsResult)
      throws NessieNotFoundException, NessieConflictException;

  EntriesResponse getEntries(
      String namedRef, String hashOnRef, Integer namespaceDepth, String filter)
      throws NessieNotFoundException;

  Branch commitMultipleOperations(String branch, String expectedHash, Operations operations)
      throws NessieNotFoundException, NessieConflictException;
}
