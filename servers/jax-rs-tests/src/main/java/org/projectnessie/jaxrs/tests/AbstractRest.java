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

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.client.ext.NessieClientFactory;
import org.projectnessie.error.BaseNessieClientServerException;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.DeltaLakeTable;
import org.projectnessie.model.Detached;
import org.projectnessie.model.DiffResponse.DiffEntry;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.IcebergView;
import org.projectnessie.model.ImmutableDeltaLakeTable;
import org.projectnessie.model.Namespace;
import org.projectnessie.model.Operation;
import org.projectnessie.model.Operation.Put;
import org.projectnessie.model.Reference;
import org.projectnessie.model.Tag;

/** See {@link AbstractTestRest} for details about and reason for the inheritance model. */
public abstract class AbstractRest {

  private NessieApiV1 api;

  // Cannot use @ExtendWith(SoftAssertionsExtension.class) + @InjectSoftAssertions here, because
  // of Quarkus class loading issues. See https://github.com/quarkusio/quarkus/issues/19814
  protected final SoftAssertions soft = new SoftAssertions();

  static {
    // Note: REST tests validate some locale-specific error messages, but expect on the messages to
    // be in ENGLISH. However, the JRE's startup classes (in particular class loaders) may cause the
    // default Locale to be initialized before Maven is able to override the user.language system
    // property. Therefore, we explicitly set the default Locale to ENGLISH here to match tests'
    // expectations.
    Locale.setDefault(Locale.ENGLISH);
  }

  @BeforeEach
  void initApi(NessieClientFactory clientFactory) {
    this.api = clientFactory.make();
  }

  public NessieApiV1 getApi() {
    return api;
  }

  @AfterEach
  public void tearDown() throws Exception {
    try {
      // Cannot use @ExtendWith(SoftAssertionsExtension.class) + @InjectSoftAssertions here, because
      // of Quarkus class loading issues. See https://github.com/quarkusio/quarkus/issues/19814
      soft.assertAll();
    } finally {
      Branch defaultBranch = api.getDefaultBranch();
      api.getAllReferences().stream()
          .forEach(
              ref -> {
                try {
                  if (ref instanceof Branch && !ref.getName().equals(defaultBranch.getName())) {
                    api.deleteBranch().branch((Branch) ref).delete();
                  } else if (ref instanceof Tag) {
                    api.deleteTag().tag((Tag) ref).delete();
                  }
                } catch (NessieConflictException | NessieNotFoundException e) {
                  throw new RuntimeException(e);
                }
              });
      api.close();
    }
  }

  protected String createCommits(
      Reference branch, int numAuthors, int commitsPerAuthor, String currentHash)
      throws BaseNessieClientServerException {
    for (int j = 0; j < numAuthors; j++) {
      String author = "author-" + j;
      for (int i = 0; i < commitsPerAuthor; i++) {
        ContentKey key = ContentKey.of("table" + i);

        Content existing =
            getApi()
                .getContent()
                .refName(branch.getName())
                .hashOnRef(currentHash)
                .key(key)
                .get()
                .get(key);

        Put op;
        if (existing != null) {
          op =
              Put.of(
                  key,
                  IcebergTable.of("some-file-" + i, 42, 42, 42, 42, existing.getId()),
                  existing);
        } else {
          op = Put.of(key, IcebergTable.of("some-file-" + i, 42, 42, 42, 42));
        }

        String nextHash =
            getApi()
                .commitMultipleOperations()
                .branchName(branch.getName())
                .hash(currentHash)
                .commitMeta(
                    CommitMeta.builder()
                        .author(author)
                        .message("committed-by-" + author)
                        .properties(ImmutableMap.of("prop1", "val1", "prop2", "val2"))
                        .build())
                .operation(op)
                .commit()
                .getHash();
        assertThat(currentHash).isNotEqualTo(nextHash);
        currentHash = nextHash;
      }
    }
    return currentHash;
  }

  protected Branch createBranch(String name, Branch from) throws BaseNessieClientServerException {
    Branch expectedBranch;
    String srcBranchName;
    if (from == null) {
      Branch main = getApi().getDefaultBranch();
      expectedBranch = Branch.of(name, main.getHash());
      srcBranchName = "main";
    } else {
      expectedBranch = Branch.of(name, from.getHash());
      srcBranchName = from.getName();
    }
    Reference created =
        getApi()
            .createReference()
            .sourceRefName(srcBranchName)
            .reference(Branch.of(name, expectedBranch.getHash()))
            .create();
    assertThat(created).isEqualTo(expectedBranch);
    return expectedBranch;
  }

  protected Branch createBranch(String name) throws BaseNessieClientServerException {
    return createBranch(name, null);
  }

  protected static void getOrCreateEmptyBranch(NessieApiV1 api, String gcBranchName) {
    try {
      api.getReference().refName(gcBranchName).get();
    } catch (NessieNotFoundException e) {
      // create a reference pointing to NO_ANCESTOR hash.
      try {
        api.createReference().reference(Branch.of(gcBranchName, null)).create();
      } catch (NessieNotFoundException | NessieConflictException ex) {
        throw new RuntimeException(ex);
      }
    }
  }

  protected void deleteBranch(String name, String hash) throws BaseNessieClientServerException {
    getApi().deleteBranch().branchName(name).hash(hash).delete();
  }

  /**
   * Used by parameterized tests to return the {@value Detached#REF_NAME}, if {@code
   * withDetachedCommit} is {@code true} or the {@link Reference#getName() reference name} from the
   * given {@code ref}.
   */
  protected static String maybeAsDetachedName(boolean withDetachedCommit, Reference ref) {
    return withDetachedCommit ? Detached.REF_NAME : ref.getName();
  }

  protected static Content contentWithoutId(Content content) {
    if (content == null) {
      return null;
    }
    if (content instanceof IcebergTable) {
      IcebergTable t = (IcebergTable) content;
      return IcebergTable.of(
          t.getMetadataLocation(),
          t.getSnapshotId(),
          t.getSchemaId(),
          t.getSpecId(),
          t.getSortOrderId());
    }
    if (content instanceof IcebergView) {
      IcebergView t = (IcebergView) content;
      return IcebergView.of(
          t.getMetadataLocation(),
          t.getVersionId(),
          t.getSchemaId(),
          t.getDialect(),
          t.getSqlText());
    }
    if (content instanceof DeltaLakeTable) {
      DeltaLakeTable t = (DeltaLakeTable) content;
      return ImmutableDeltaLakeTable.builder().from(t).id(null).build();
    }
    if (content instanceof Namespace) {
      Namespace t = (Namespace) content;
      return Namespace.of(t.getElements());
    }
    throw new IllegalArgumentException(content.toString());
  }

  protected static Operation operationWithoutContentId(Operation op) {
    if (op instanceof Put) {
      Put put = (Put) op;
      return put.getExpectedContent() != null
          ? Put.of(
              put.getKey(),
              contentWithoutId(put.getContent()),
              contentWithoutId(put.getExpectedContent()))
          : Put.of(put.getKey(), contentWithoutId(put.getContent()));
    }
    return op;
  }

  protected static DiffEntry diffEntryWithoutContentId(DiffEntry diff) {
    if (diff == null) {
      return null;
    }
    return DiffEntry.diffEntry(
        diff.getKey(), contentWithoutId(diff.getFrom()), contentWithoutId(diff.getTo()));
  }

  protected static List<DiffEntry> diffEntriesWithoutContentId(List<DiffEntry> diff) {
    if (diff == null) {
      return null;
    }
    return diff.stream().map(AbstractRest::diffEntryWithoutContentId).collect(Collectors.toList());
  }

  protected static List<Operation> operationsWithoutContentId(List<Operation> operations) {
    if (operations == null) {
      return null;
    }
    return operations.stream()
        .map(AbstractRest::operationWithoutContentId)
        .collect(Collectors.toList());
  }
  /**
   * Enum intended to be used a test method parameter to transform a {@link Reference} in multiple
   * ways.
   */
  enum ReferenceMode {
    /** Removes the {@link Reference#getHash()} from the reference. */
    NAME_ONLY {
      @Override
      Reference transform(Reference ref) {
        switch (ref.getType()) {
          case TAG:
            return Tag.of(ref.getName(), null);
          case BRANCH:
            return Branch.of(ref.getName(), null);
          default:
            throw new IllegalArgumentException(ref.toString());
        }
      }
    },
    /** Keep the reference unchanged. */
    UNCHANGED {
      @Override
      Reference transform(Reference ref) {
        return ref;
      }
    },
    /**
     * Make the reference a {@link Detached} with its {@link Detached#getHash()} using the hash of
     * the given reference.
     */
    DETACHED {
      @Override
      Reference transform(Reference ref) {
        return Detached.of(ref.getHash());
      }
    };

    abstract Reference transform(Reference ref);
  }
}
