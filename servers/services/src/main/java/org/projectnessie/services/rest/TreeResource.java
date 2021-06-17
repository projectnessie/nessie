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
package org.projectnessie.services.rest;

import static org.projectnessie.cel.common.types.Err.newTypeConversionError;

import com.google.api.expr.v1alpha1.Decl;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.security.Principal;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.enterprise.context.RequestScoped;
import javax.inject.Inject;
import org.projectnessie.api.TreeApi;
import org.projectnessie.api.params.CommitLogParams;
import org.projectnessie.api.params.EntriesParams;
import org.projectnessie.cel.checker.Decls;
import org.projectnessie.cel.common.types.TypeT;
import org.projectnessie.cel.common.types.ref.BaseVal;
import org.projectnessie.cel.common.types.ref.Val;
import org.projectnessie.cel.tools.Script;
import org.projectnessie.cel.tools.ScriptException;
import org.projectnessie.cel.tools.ScriptHost;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Contents;
import org.projectnessie.model.Contents.Type;
import org.projectnessie.model.ContentsKey;
import org.projectnessie.model.EntriesResponse;
import org.projectnessie.model.ImmutableBranch;
import org.projectnessie.model.ImmutableCommitMeta;
import org.projectnessie.model.ImmutableHash;
import org.projectnessie.model.ImmutableLogResponse;
import org.projectnessie.model.ImmutableTag;
import org.projectnessie.model.LogResponse;
import org.projectnessie.model.Merge;
import org.projectnessie.model.Operation;
import org.projectnessie.model.Operations;
import org.projectnessie.model.Reference;
import org.projectnessie.model.Tag;
import org.projectnessie.model.Transplant;
import org.projectnessie.services.cel.CELTranslator;
import org.projectnessie.services.config.ServerConfig;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Delete;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.NamedRef;
import org.projectnessie.versioned.Put;
import org.projectnessie.versioned.Ref;
import org.projectnessie.versioned.ReferenceAlreadyExistsException;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.TagName;
import org.projectnessie.versioned.Unchanged;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.WithHash;

/** REST endpoint for trees. */
@RequestScoped
public class TreeResource extends BaseResource implements TreeApi {

  private static final int MAX_COMMIT_LOG_ENTRIES = 250;
  private static final ScriptHost SCRIPT_HOST = ScriptHost.newBuilder().build();
  private static final List<Decl> COMMIT_LOG_DECLARATIONS =
      Arrays.asList(Decls.newVar("commit", Decls.newMapType(Decls.String, Decls.String)));
  private static final List<Decl> ENTRIES_DECLARATIONS =
      Arrays.asList(Decls.newVar("entry", Decls.newMapType(Decls.String, Decls.String)));

  @Inject
  public TreeResource(
      ServerConfig config,
      Principal principal,
      VersionStore<Contents, CommitMeta, Contents.Type> store) {
    super(config, principal, store);
  }

  @Override
  public List<Reference> getAllReferences() {
    try (Stream<WithHash<NamedRef>> str = getStore().getNamedRefs()) {
      return str.map(TreeResource::makeNamedRef).collect(Collectors.toList());
    }
  }

  @Override
  public Reference getReferenceByName(String refName) throws NessieNotFoundException {
    try {
      return makeRef(getStore().toRef(refName));
    } catch (ReferenceNotFoundException e) {
      throw new NessieNotFoundException(
          String.format("Unable to find reference [%s].", refName), e);
    }
  }

  @Override
  public Reference createReference(Reference reference)
      throws NessieNotFoundException, NessieConflictException {
    final NamedRef namedReference;
    if (reference instanceof Branch) {
      namedReference = BranchName.of(reference.getName());
      Hash hash = createReference(namedReference, reference.getHash());
      return Branch.of(reference.getName(), hash.asString());
    } else if (reference instanceof Tag) {
      namedReference = TagName.of(reference.getName());
      Hash hash = createReference(namedReference, reference.getHash());
      return Tag.of(reference.getName(), hash.asString());
    } else {
      throw new IllegalArgumentException("Only tag and branch references can be created");
    }
  }

  private Hash createReference(NamedRef reference, String hash)
      throws NessieNotFoundException, NessieConflictException {
    try {
      return getStore().create(reference, toHash(hash, false));
    } catch (ReferenceNotFoundException e) {
      throw new NessieNotFoundException("Failure while searching for provided targeted hash.", e);
    } catch (ReferenceAlreadyExistsException e) {
      throw new NessieConflictException(
          String.format("A reference of name [%s] already exists.", reference.getName()), e);
    }
  }

  @Override
  public Branch getDefaultBranch() throws NessieNotFoundException {
    Reference r = getReferenceByName(getConfig().getDefaultBranch());
    if (!(r instanceof Branch)) {
      throw new IllegalStateException("Default branch isn't a branch");
    }
    return (Branch) r;
  }

  @Override
  public void assignTag(String tagName, String expectedHash, Tag tag)
      throws NessieNotFoundException, NessieConflictException {
    assignReference(TagName.of(tagName), expectedHash, tag.getHash());
  }

  @Override
  public void deleteTag(String tagName, String hash)
      throws NessieConflictException, NessieNotFoundException {
    deleteReference(TagName.of(tagName), hash);
  }

  @Override
  public void assignBranch(String branchName, String expectedHash, Branch branch)
      throws NessieNotFoundException, NessieConflictException {
    assignReference(BranchName.of(branchName), expectedHash, branch.getHash());
  }

  @Override
  public void deleteBranch(String branchName, String hash)
      throws NessieConflictException, NessieNotFoundException {
    deleteReference(BranchName.of(branchName), hash);
  }

  @Override
  public LogResponse getCommitLog(String ref, CommitLogParams params)
      throws NessieNotFoundException {
    int max =
        Math.min(
            params.getMaxRecords() != null ? params.getMaxRecords() : MAX_COMMIT_LOG_ENTRIES,
            MAX_COMMIT_LOG_ENTRIES);
    Hash startRef = getHashOrThrow(params.getPageToken() != null ? params.getPageToken() : ref);

    if (null != params.getCelExpr()) {
      Preconditions.checkArgument(
          params.getAuthors().isEmpty(), "Cannot combine 'cel_expr' with the 'authors' parameter");
      Preconditions.checkArgument(
          params.getCommitters().isEmpty(),
          "Cannot combine 'cel_expr' with the 'committers' parameter");
      Preconditions.checkArgument(
          null == params.getAfter(), "Cannot combine 'cel_expr' with the 'after' parameter");
      Preconditions.checkArgument(
          null == params.getBefore(), "Cannot combine 'cel_expr' with the 'before' parameter");
    }

    try (Stream<ImmutableCommitMeta> s =
        getStore()
            .getCommits(startRef)
            .map(cwh -> cwh.getValue().toBuilder().hash(cwh.getHash().asString()).build())) {

      List<CommitMeta> items;

      items =
          filterCommitLogWithCelExpression(s, params).limit(max + 1).collect(Collectors.toList());

      if (items.size() == max + 1) {
        return ImmutableLogResponse.builder()
            .addAllOperations(items.subList(0, max))
            .hasMore(true)
            .token(items.get(max).getHash())
            .build();
      }
      return ImmutableLogResponse.builder().addAllOperations(items).build();
    } catch (ReferenceNotFoundException e) {
      throw new NessieNotFoundException(
          String.format("Unable to find the requested ref [%s].", ref), e);
    }
  }

  private Stream<ImmutableCommitMeta> filterCommitLogWithCelExpression(
      Stream<ImmutableCommitMeta> items, CommitLogParams params) {
    System.out.println("params = " + params);
    String expr = CELTranslator.from(params);
    System.out.println("expr = " + expr);
    final String celExpr = "".equals(expr) ? params.getCelExpr() : expr;
    System.out.println("celExpr = " + celExpr);
    System.out.println("--------------->");
    if (null == celExpr) {
      return items;
    }
    final Script script;
    try {
      script =
          SCRIPT_HOST.getOrCreateScript(celExpr, COMMIT_LOG_DECLARATIONS, Collections.emptyList());
    } catch (ScriptException e) {
      throw new RuntimeException(e);
    }
    return items.filter(
        commit -> {
          // currently this is just a workaround where we put CommitMeta into a hash structure.
          // Eventually we should have a CommitMetaT that we register to avoid unnecessary object
          // creation
          Map<String, Object> arguments =
              ImmutableMap.of(
                  "commit",
                  ImmutableMap.of(
                      "author",
                      commit.getAuthor(),
                      "committer",
                      commit.getCommitter(),
                      "commitTime",
                      commit.getCommitTime()));

          try {
            return script.execute(Boolean.class, arguments);
          } catch (ScriptException e) {
            throw new RuntimeException(e);
          }
        });
  }

  /**
   * Applies different filters to the {@link Stream} of commits based on the settings in {@link
   * CommitLogParams}.
   *
   * @param commits The commit log that different filters will be applied to
   * @param params The commit log filter parameters
   * @return A potentially filtered {@link Stream} of commits based on {@link CommitLogParams}
   */
  private Stream<ImmutableCommitMeta> filterCommitLog(
      Stream<ImmutableCommitMeta> commits, CommitLogParams params) {
    if (null != params.getAuthors() && !params.getAuthors().isEmpty()) {
      commits = commits.filter(commit -> params.getAuthors().contains(commit.getAuthor()));
    }
    if (null != params.getCommitters() && !params.getCommitters().isEmpty()) {
      commits = commits.filter(commit -> params.getCommitters().contains(commit.getCommitter()));
    }
    if (null != params.getAfter()) {
      commits =
          commits.filter(
              commit ->
                  null != commit.getCommitTime()
                      && commit.getCommitTime().isAfter(params.getAfter()));
    }
    if (null != params.getBefore()) {
      commits =
          commits.filter(
              commit ->
                  null != commit.getCommitTime()
                      && commit.getCommitTime().isBefore(params.getBefore()));
    }
    return commits;
  }

  @Override
  public void transplantCommitsIntoBranch(
      String branchName, String hash, String message, Transplant transplant)
      throws NessieNotFoundException, NessieConflictException {
    try {
      List<Hash> transplants;
      try (Stream<Hash> s = transplant.getHashesToTransplant().stream().map(Hash::of)) {
        transplants = s.collect(Collectors.toList());
      }
      getStore().transplant(BranchName.of(branchName), toHash(hash, true), transplants);
    } catch (ReferenceNotFoundException e) {
      throw new NessieNotFoundException(
          String.format(
              "Unable to find the requested branch we're transplanting to of [%s].", branchName),
          e);
    } catch (ReferenceConflictException e) {
      throw new NessieConflictException(
          String.format(
              "The hash provided %s does not match the current status of the branch %s.",
              hash, branchName),
          e);
    }
  }

  @Override
  public void mergeRefIntoBranch(String branchName, String hash, Merge merge)
      throws NessieNotFoundException, NessieConflictException {
    try {
      getStore()
          .merge(
              toHash(merge.getFromHash(), true).get(),
              BranchName.of(branchName),
              toHash(hash, true));
    } catch (ReferenceNotFoundException e) {
      throw new NessieNotFoundException(
          String.format("At least one of the references provided does not exist."), e);
    } catch (ReferenceConflictException e) {
      throw new NessieConflictException(
          String.format("The branch [%s] does not have the expected hash [%s].", branchName, hash),
          e);
    }
  }

  @Override
  public EntriesResponse getEntries(String refName, EntriesParams params)
      throws NessieNotFoundException {

    final Hash hash = getHashOrThrow(refName);
    // TODO Implement paging. At the moment, we do not expect that many keys/entries to be returned.
    //  So the size of the whole result is probably reasonable and unlikely to "kill" either the
    //  server or client. We have to figure out _how_ to implement paging for keys/entries, i.e.
    //  whether we shall just do the whole computation for a specific hash for every page or have
    //  a more sophisticated approach, potentially with support from the (tiered-)version-store.
    //  note currently we are filtering types at the REST level. This could in theory be pushed down
    // to the store though
    //  all existing VersionStore implementations have to read all keys anyways so we don't get much
    try {
      List<EntriesResponse.Entry> entries;
      try (Stream<EntriesResponse.Entry> s =
          getStore()
              .getKeys(hash)
              .map(
                  key ->
                      EntriesResponse.Entry.builder()
                          .name(fromKey(key.getValue()))
                          .type((Type) key.getType())
                          .build())) {
        entries =
            filterEntriesWithCelExpression(s, params).collect(ImmutableList.toImmutableList());
      }
      return EntriesResponse.builder().addAllEntries(entries).build();
    } catch (ReferenceNotFoundException e) {
      throw new NessieNotFoundException(
          String.format("Unable to find the reference [%s].", refName), e);
    }
  }

  /**
   * Applies different filters to the {@link Stream} of entries based on the settings in {@link
   * EntriesParams}.
   *
   * @param entries The entries that different filters will be applied to
   * @param params The filter parameters for the entries
   * @return A potentially filtered {@link Stream} of entries based on {@link EntriesParams}
   */
  private Stream<EntriesResponse.Entry> filterEntries(
      Stream<EntriesResponse.Entry> entries, EntriesParams params) {
    final Set<Type> payloads;
    if (params.getTypes().isEmpty()) {
      payloads = Arrays.stream(Type.values()).collect(Collectors.toSet());
    } else {
      payloads = params.getTypes().stream().map(Type::valueOf).collect(Collectors.toSet());
    }

    entries = entries.filter(x -> payloads.contains(x.getType()));

    if (null != params.getNamespace()) {
      entries =
          entries.filter(x -> x.getName().getNamespace().name().startsWith(params.getNamespace()));
    }
    return entries;
  }

  private Stream<EntriesResponse.Entry> filterEntriesWithCelExpression(
      Stream<EntriesResponse.Entry> entries, EntriesParams params) {
    System.out.println("params = " + params);
    String expr = CELTranslator.from(params);
    System.out.println("expr = " + expr);
    final String celExpr = "".equals(expr) ? params.getCelExpr() : expr;
    System.out.println("celExpr = " + celExpr);
    System.out.println("--------------->");
    if (null == celExpr) {
      return entries;
    }
    final Script script;
    try {
      script =
          SCRIPT_HOST.getOrCreateScript(celExpr, ENTRIES_DECLARATIONS, Collections.emptyList());
    } catch (ScriptException e) {
      throw new RuntimeException(e);
    }
    return entries.filter(
        entry -> {
          // currently this is just a workaround where we put EntriesResponse.Entry into a hash
          // structure.
          // Eventually we should have a EntriesT that we register to avoid unnecessary object
          // creation
          Map<String, Object> arguments =
              ImmutableMap.of(
                  "entry",
                  ImmutableMap.of(
                      "namespace",
                      entry.getName().getNamespace().name(),
                      "contentType",
                      entry.getType().name()));

          try {
            return script.execute(Boolean.class, arguments);
          } catch (ScriptException e) {
            throw new RuntimeException(e);
          }
        });
  }

  @Override
  public Branch commitMultipleOperations(String branch, String hash, Operations operations)
      throws NessieNotFoundException, NessieConflictException {
    List<org.projectnessie.versioned.Operation<Contents>> ops =
        operations.getOperations().stream()
            .map(TreeResource::toOp)
            .collect(ImmutableList.toImmutableList());
    String newHash = doOps(branch, hash, operations.getCommitMeta(), ops).asString();
    return Branch.of(branch, newHash);
  }

  private static Optional<Hash> toHash(String hash, boolean required)
      throws NessieConflictException {
    if (hash == null || hash.isEmpty()) {
      if (required) {
        throw new NessieConflictException("Must provide expected hash value for operation.");
      }
      return Optional.empty();
    }
    return Optional.of(Hash.of(hash));
  }

  private void deleteReference(NamedRef name, String hash)
      throws NessieConflictException, NessieNotFoundException {
    try {
      getStore().delete(name, toHash(hash, true));
    } catch (ReferenceNotFoundException e) {
      throw new NessieNotFoundException(
          String.format("Unable to find reference [%s] to delete.", name.getName()), e);
    } catch (ReferenceConflictException e) {
      throw new NessieConflictException(
          String.format(
              "The hash provided %s does not match the current status of the reference %s.",
              hash, name.getName()),
          e);
    }
  }

  private void assignReference(NamedRef ref, String oldHash, String newHash)
      throws NessieNotFoundException, NessieConflictException {
    try {
      WithHash<Ref> resolved = getStore().toRef(ref.getName());
      Ref resolvedRef = resolved.getValue();
      if (resolvedRef instanceof NamedRef) {
        getStore()
            .assign(
                (NamedRef) resolvedRef,
                toHash(oldHash, true),
                toHash(newHash, true)
                    .orElseThrow(
                        () ->
                            new NessieConflictException(
                                "Must provide target hash value for operation.")));
      } else {
        throw new IllegalArgumentException("Can only assign branch and tag types.");
      }
    } catch (ReferenceNotFoundException e) {
      throw new NessieNotFoundException("Unable to find a ref or hash provided.", e);
    } catch (ReferenceConflictException e) {
      throw new NessieConflictException(
          String.format(
              "The hash provided %s does not match the current status of the reference %s.",
              oldHash, ref),
          e);
    }
  }

  private static ContentsKey fromKey(Key key) {
    return ContentsKey.of(key.getElements());
  }

  private static Reference makeNamedRef(WithHash<NamedRef> refWithHash) {
    return makeRef(refWithHash);
  }

  private static Reference makeRef(WithHash<? extends Ref> refWithHash) {
    Ref ref = refWithHash.getValue();
    // todo do we want to send back Hash object or the string. I don't want internal API escaping so
    // maybe an external representation of hash
    if (ref instanceof TagName) {
      return ImmutableTag.builder()
          .name(((NamedRef) ref).getName())
          .hash(refWithHash.getHash().asString())
          .build();
    } else if (ref instanceof BranchName) {
      return ImmutableBranch.builder()
          .name(((NamedRef) ref).getName())
          .hash(refWithHash.getHash().asString())
          .build();
    } else if (ref instanceof Hash) {
      String hash = refWithHash.getHash().asString();
      return ImmutableHash.builder().name(hash).build();
    } else {
      throw new UnsupportedOperationException("only converting tags or branches"); // todo
    }
  }

  private static org.projectnessie.versioned.Operation<Contents> toOp(Operation o) {
    Key key = Key.of(o.getKey().getElements().toArray(new String[0]));
    if (o instanceof Operation.Delete) {
      return Delete.of(key);
    } else if (o instanceof Operation.Put) {
      return Put.of(key, ((Operation.Put) o).getContents());
    } else if (o instanceof Operation.Unchanged) {
      return Unchanged.of(key);
    } else {
      throw new IllegalStateException("Unknown operation " + o);
    }
  }

  private static class CommitMetaT extends BaseVal {
    private final CommitMeta commit;

    /** CommitMetaT singleton. */
    public static final TypeT CommitMetaType = TypeT.newTypeValue("commitMeta");

    private CommitMetaT(CommitMeta commit) {
      this.commit = commit;
    }

    public static CommitMetaT of(CommitMeta commit) {
      return new CommitMetaT(commit);
    }

    @Override
    public <T> T convertToNative(Class<T> typeDesc) {
      if (typeDesc == CommitMeta.class
          || typeDesc == ImmutableCommitMeta.class
          || typeDesc == Object.class) {
        return (T) commit;
      }
      if (typeDesc == Val.class || typeDesc == CommitMetaT.class) {
        return (T) this;
      }
      throw new RuntimeException(
          String.format(
              "native type conversion error from '%s' to '%s'",
              CommitMetaType, typeDesc.getName()));
    }

    @Override
    public Val convertToType(org.projectnessie.cel.common.types.ref.Type typeVal) {
      if (typeVal == CommitMetaType) {
        return this;
      }
      return newTypeConversionError(CommitMetaType, typeVal);
    }

    @Override
    public Val equal(Val other) {
      return null;
    }

    @Override
    public org.projectnessie.cel.common.types.ref.Type type() {
      return CommitMetaType;
    }

    @Override
    public Object value() {
      return commit;
    }
  }
}
