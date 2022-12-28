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
import javax.validation.Valid;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
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
import org.projectnessie.model.Validation;

/**
 * Server-side interface to services managing the content trees.
 *
 * <p>Refer to the javadoc of corresponding client-facing interfaces in the {@code model} module for
 * the meaning of various methods and their parameters.
 */
public interface TreeService {

  Branch getDefaultBranch() throws NessieNotFoundException;

  ReferencesResponse getAllReferences(FetchOption fetchOption, @Nullable String filter);

  Reference getReferenceByName(
      @Valid
          @NotNull
          @Pattern(regexp = Validation.REF_NAME_REGEX, message = Validation.REF_NAME_MESSAGE)
          String refName,
      FetchOption fetchOption)
      throws NessieNotFoundException;

  Reference createReference(
      @Valid
          @NotNull
          @Pattern(regexp = Validation.REF_NAME_REGEX, message = Validation.REF_NAME_MESSAGE)
          String refName,
      Reference.ReferenceType type,
      @Valid @Pattern(regexp = Validation.HASH_REGEX, message = Validation.HASH_MESSAGE)
          String hash,
      @Valid @Pattern(regexp = Validation.REF_NAME_REGEX, message = Validation.REF_NAME_MESSAGE)
          String sourceRefName)
      throws NessieNotFoundException, NessieConflictException;

  Reference assignReference(
      Reference.ReferenceType referenceType,
      @Valid
          @NotNull
          @Pattern(regexp = Validation.REF_NAME_REGEX, message = Validation.REF_NAME_MESSAGE)
          String referenceName,
      @Valid @Pattern(regexp = Validation.HASH_REGEX, message = Validation.HASH_MESSAGE)
          String expectedHash,
      @Valid Reference assignTo)
      throws NessieNotFoundException, NessieConflictException;

  void deleteReference(
      Reference.ReferenceType referenceType,
      @Valid
          @NotNull
          @Pattern(regexp = Validation.REF_NAME_REGEX, message = Validation.REF_NAME_MESSAGE)
          String referenceName,
      @Valid @Pattern(regexp = Validation.HASH_REGEX, message = Validation.HASH_MESSAGE)
          String expectedHash)
      throws NessieConflictException, NessieNotFoundException;

  LogResponse getCommitLog(
      @Valid
          @NotNull
          @Pattern(regexp = Validation.REF_NAME_REGEX, message = Validation.REF_NAME_MESSAGE)
          String namedRef,
      FetchOption fetchOption,
      @Valid @Pattern(regexp = Validation.HASH_REGEX, message = Validation.HASH_MESSAGE)
          String oldestHashLimit,
      @Valid @Pattern(regexp = Validation.HASH_REGEX, message = Validation.HASH_MESSAGE)
          String youngestHash,
      @Nullable String filter,
      @Nullable Integer maxRecords,
      @Nullable String pageToken)
      throws NessieNotFoundException;

  MergeResponse transplantCommitsIntoBranch(
      @Valid
          @NotNull
          @Pattern(regexp = Validation.REF_NAME_REGEX, message = Validation.REF_NAME_MESSAGE)
          String branchName,
      @Valid @NotNull @Pattern(regexp = Validation.HASH_REGEX, message = Validation.HASH_MESSAGE)
          String expectedHash,
      String message,
      List<String> hashesToTransplant,
      @Valid
          @NotBlank
          @NotNull
          @Pattern(regexp = Validation.REF_NAME_REGEX, message = Validation.REF_NAME_MESSAGE)
          String fromRefName,
      Boolean keepIndividualCommits,
      Collection<MergeKeyBehavior> keyMergeTypes,
      MergeBehavior defaultMergeType,
      @Nullable Boolean dryRun,
      @Nullable Boolean fetchAdditionalInfo,
      @Nullable Boolean returnConflictAsResult)
      throws NessieNotFoundException, NessieConflictException;

  MergeResponse mergeRefIntoBranch(
      @Valid
          @NotNull
          @Pattern(regexp = Validation.REF_NAME_REGEX, message = Validation.REF_NAME_MESSAGE)
          String branchName,
      @Valid @NotNull @Pattern(regexp = Validation.HASH_REGEX, message = Validation.HASH_MESSAGE)
          String expectedHash,
      @Valid
          @NotBlank
          @Pattern(regexp = Validation.REF_NAME_REGEX, message = Validation.REF_NAME_MESSAGE)
          String fromRefName,
      @Valid @NotBlank @Pattern(regexp = Validation.HASH_REGEX, message = Validation.HASH_MESSAGE)
          String fromHash,
      @Nullable Boolean keepIndividualCommits,
      @Nullable String message,
      Collection<MergeKeyBehavior> keyMergeTypes,
      MergeBehavior defaultMergeType,
      @Nullable Boolean dryRun,
      @Nullable Boolean fetchAdditionalInfo,
      @Nullable Boolean returnConflictAsResult)
      throws NessieNotFoundException, NessieConflictException;

  EntriesResponse getEntries(
      @Valid
          @NotNull
          @Pattern(regexp = Validation.REF_NAME_REGEX, message = Validation.REF_NAME_MESSAGE)
          String namedRef,
      @Valid @Nullable @Pattern(regexp = Validation.HASH_REGEX, message = Validation.HASH_MESSAGE)
          String hashOnRef,
      @Nullable Integer namespaceDepth,
      @Nullable String filter)
      throws NessieNotFoundException;

  Branch commitMultipleOperations(
      @Valid
          @NotNull
          @Pattern(regexp = Validation.REF_NAME_REGEX, message = Validation.REF_NAME_MESSAGE)
          String branch,
      @Valid @NotNull @Pattern(regexp = Validation.HASH_REGEX, message = Validation.HASH_MESSAGE)
          String expectedHash,
      @Valid Operations operations)
      throws NessieNotFoundException, NessieConflictException;
}
