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

import static org.projectnessie.model.Validation.HASH_OR_RELATIVE_COMMIT_SPEC_MESSAGE;
import static org.projectnessie.model.Validation.HASH_OR_RELATIVE_COMMIT_SPEC_REGEX;
import static org.projectnessie.model.Validation.REF_NAME_MESSAGE;
import static org.projectnessie.model.Validation.REF_NAME_REGEX;

import jakarta.annotation.Nullable;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.CommitResponse;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.EntriesResponse.Entry;
import org.projectnessie.model.FetchOption;
import org.projectnessie.model.LogResponse.LogEntry;
import org.projectnessie.model.MergeBehavior;
import org.projectnessie.model.MergeKeyBehavior;
import org.projectnessie.model.MergeResponse;
import org.projectnessie.model.Operations;
import org.projectnessie.model.Reference;
import org.projectnessie.model.Reference.ReferenceType;
import org.projectnessie.model.ReferenceHistoryResponse;
import org.projectnessie.versioned.NamedRef;
import org.projectnessie.versioned.RequestMeta;
import org.projectnessie.versioned.WithHash;

/**
 * Server-side interface to services managing the content trees.
 *
 * <p>Refer to the javadoc of corresponding client-facing interfaces in the {@code model} module for
 * the meaning of various methods and their parameters.
 */
public interface TreeService {

  int MAX_COMMIT_LOG_ENTRIES = 250;

  Branch getDefaultBranch() throws NessieNotFoundException;

  <R> R getAllReferences(
      FetchOption fetchOption,
      @Nullable String filter,
      String pagingToken,
      PagedResponseHandler<R, Reference> pagedResponseHandler);

  Reference getReferenceByName(
      @Valid @NotNull @Pattern(regexp = REF_NAME_REGEX, message = REF_NAME_MESSAGE) String refName,
      FetchOption fetchOption)
      throws NessieNotFoundException;

  ReferenceHistoryResponse getReferenceHistory(
      @Valid @NotNull @Pattern(regexp = REF_NAME_REGEX, message = REF_NAME_MESSAGE) String refName,
      Integer headCommitsToScan)
      throws NessieNotFoundException;

  Reference createReference(
      @Valid @NotNull @Pattern(regexp = REF_NAME_REGEX, message = REF_NAME_MESSAGE) String refName,
      ReferenceType type,
      @Valid
          @Pattern(
              regexp = HASH_OR_RELATIVE_COMMIT_SPEC_REGEX,
              message = HASH_OR_RELATIVE_COMMIT_SPEC_MESSAGE)
          String hash,
      @Valid @Pattern(regexp = REF_NAME_REGEX, message = REF_NAME_MESSAGE) String sourceRefName)
      throws NessieNotFoundException, NessieConflictException;

  Reference assignReference(
      ReferenceType referenceType,
      @Valid @NotNull @Pattern(regexp = REF_NAME_REGEX, message = REF_NAME_MESSAGE)
          String referenceName,
      @Valid
          @Pattern(
              regexp = HASH_OR_RELATIVE_COMMIT_SPEC_REGEX,
              message = HASH_OR_RELATIVE_COMMIT_SPEC_MESSAGE)
          String expectedHash,
      @Valid Reference assignTo)
      throws NessieNotFoundException, NessieConflictException;

  Reference deleteReference(
      ReferenceType referenceType,
      @Valid @NotNull @Pattern(regexp = REF_NAME_REGEX, message = REF_NAME_MESSAGE)
          String referenceName,
      @Valid
          @Pattern(
              regexp = HASH_OR_RELATIVE_COMMIT_SPEC_REGEX,
              message = HASH_OR_RELATIVE_COMMIT_SPEC_MESSAGE)
          String expectedHash)
      throws NessieConflictException, NessieNotFoundException;

  <R> R getCommitLog(
      @Valid @NotNull @Pattern(regexp = REF_NAME_REGEX, message = REF_NAME_MESSAGE) String namedRef,
      FetchOption fetchOption,
      @Valid
          @Pattern(
              regexp = HASH_OR_RELATIVE_COMMIT_SPEC_REGEX,
              message = HASH_OR_RELATIVE_COMMIT_SPEC_MESSAGE)
          String oldestHashLimit,
      @Valid
          @Pattern(
              regexp = HASH_OR_RELATIVE_COMMIT_SPEC_REGEX,
              message = HASH_OR_RELATIVE_COMMIT_SPEC_MESSAGE)
          String youngestHash,
      @Nullable String filter,
      @Nullable String pageToken,
      @NotNull PagedResponseHandler<R, LogEntry> pagedResponseHandler)
      throws NessieNotFoundException;

  MergeResponse transplantCommitsIntoBranch(
      @Valid @NotNull @Pattern(regexp = REF_NAME_REGEX, message = REF_NAME_MESSAGE)
          String branchName,
      @Valid
          @Pattern(
              regexp = HASH_OR_RELATIVE_COMMIT_SPEC_REGEX,
              message = HASH_OR_RELATIVE_COMMIT_SPEC_MESSAGE)
          String expectedHash,
      @Nullable CommitMeta commitMeta,
      List<
              @Pattern(
                  regexp = HASH_OR_RELATIVE_COMMIT_SPEC_REGEX,
                  message = HASH_OR_RELATIVE_COMMIT_SPEC_MESSAGE)
              String>
          hashesToTransplant,
      @Valid @NotBlank @NotNull @Pattern(regexp = REF_NAME_REGEX, message = REF_NAME_MESSAGE)
          String fromRefName,
      Collection<MergeKeyBehavior> keyMergeBehaviors,
      MergeBehavior defaultMergeType,
      @Nullable Boolean dryRun,
      @Nullable Boolean fetchAdditionalInfo,
      @Nullable Boolean returnConflictAsResult)
      throws NessieNotFoundException, NessieConflictException;

  MergeResponse mergeRefIntoBranch(
      @Valid @NotNull @Pattern(regexp = REF_NAME_REGEX, message = REF_NAME_MESSAGE)
          String branchName,
      @Valid
          @Pattern(
              regexp = HASH_OR_RELATIVE_COMMIT_SPEC_REGEX,
              message = HASH_OR_RELATIVE_COMMIT_SPEC_MESSAGE)
          String expectedHash,
      @Valid @NotBlank @Pattern(regexp = REF_NAME_REGEX, message = REF_NAME_MESSAGE)
          String fromRefName,
      @Valid
          @Pattern(
              regexp = HASH_OR_RELATIVE_COMMIT_SPEC_REGEX,
              message = HASH_OR_RELATIVE_COMMIT_SPEC_MESSAGE)
          String fromHash,
      @Nullable CommitMeta commitMeta,
      Collection<MergeKeyBehavior> keyMergeBehaviors,
      MergeBehavior defaultMergeType,
      @Nullable Boolean dryRun,
      @Nullable Boolean fetchAdditionalInfo,
      @Nullable Boolean returnConflictAsResult)
      throws NessieNotFoundException, NessieConflictException;

  <R> R getEntries(
      @Valid @NotNull @Pattern(regexp = REF_NAME_REGEX, message = REF_NAME_MESSAGE) String namedRef,
      @Valid
          @Nullable
          @Pattern(
              regexp = HASH_OR_RELATIVE_COMMIT_SPEC_REGEX,
              message = HASH_OR_RELATIVE_COMMIT_SPEC_MESSAGE)
          String hashOnRef,
      @Nullable Integer namespaceDepth,
      @Nullable String filter,
      @Nullable String pagingToken,
      boolean withContent,
      PagedResponseHandler<R, Entry> pagedResponseHandler,
      Consumer<WithHash<NamedRef>> effectiveReference,
      @Nullable ContentKey minKey,
      @Nullable ContentKey maxKey,
      ContentKey prefixKey,
      List<ContentKey> requestedKeys)
      throws NessieNotFoundException;

  CommitResponse commitMultipleOperations(
      @Valid @NotNull @Pattern(regexp = REF_NAME_REGEX, message = REF_NAME_MESSAGE) String branch,
      @Valid
          @Pattern(
              regexp = HASH_OR_RELATIVE_COMMIT_SPEC_REGEX,
              message = HASH_OR_RELATIVE_COMMIT_SPEC_MESSAGE)
          String expectedHash,
      @Valid Operations operations,
      @NotNull RequestMeta requestMeta)
      throws NessieNotFoundException, NessieConflictException;
}
