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

import static java.lang.Integer.parseInt;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.projectnessie.versioned.storage.common.logic.Logics.repositoryLogic;
import static org.projectnessie.versioned.storage.common.persist.Reference.reference;
import static org.projectnessie.versioned.transfer.ExportImportConstants.EXPORT_METADATA;
import static org.projectnessie.versioned.transfer.ExportImportConstants.HEADS_AND_FORKS;
import static org.projectnessie.versioned.transfer.ExportImportTestUtil.intToObjId;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.BitSet;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.projectnessie.api.NessieVersion;
import org.projectnessie.versioned.storage.common.config.StoreConfig;
import org.projectnessie.versioned.storage.common.exceptions.ObjNotFoundException;
import org.projectnessie.versioned.storage.common.exceptions.ObjTooLargeException;
import org.projectnessie.versioned.storage.common.exceptions.RefAlreadyExistsException;
import org.projectnessie.versioned.storage.common.exceptions.RefConditionFailedException;
import org.projectnessie.versioned.storage.common.exceptions.RefNotFoundException;
import org.projectnessie.versioned.storage.common.logic.IndexesLogic;
import org.projectnessie.versioned.storage.common.logic.PagedResult;
import org.projectnessie.versioned.storage.common.logic.PagingToken;
import org.projectnessie.versioned.storage.common.logic.ReferenceLogic;
import org.projectnessie.versioned.storage.common.logic.RepositoryDescription;
import org.projectnessie.versioned.storage.common.logic.RepositoryLogic;
import org.projectnessie.versioned.storage.common.objtypes.CommitObj;
import org.projectnessie.versioned.storage.common.objtypes.UpdateableObj;
import org.projectnessie.versioned.storage.common.persist.CloseableIterator;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.ObjType;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.common.persist.Reference;
import org.projectnessie.versioned.storage.inmemory.InmemoryBackend;
import org.projectnessie.versioned.storage.inmemory.InmemoryBackendFactory;
import org.projectnessie.versioned.storage.testextension.NessieBackendName;
import org.projectnessie.versioned.storage.testextension.NessiePersist;
import org.projectnessie.versioned.storage.testextension.PersistExtension;
import org.projectnessie.versioned.transfer.files.ExportFileSupplier;
import org.projectnessie.versioned.transfer.files.ImportFileSupplier;
import org.projectnessie.versioned.transfer.serialize.TransferTypes;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.ExportMeta;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.ExportVersion;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.HeadsAndForks;

@ExtendWith({PersistExtension.class, SoftAssertionsExtension.class})
@NessieBackendName(InmemoryBackendFactory.NAME)
public abstract class AbstractExportImport {
  @InjectSoftAssertions private SoftAssertions soft;

  @Test
  public void fileSupplierMixed(@TempDir Path targetDir, @NessiePersist Persist persist)
      throws Exception {
    repositoryLogic(persist).initialize("main");

    try (var exportFileSupplier = prepareExporter(targetDir)) {
      var exporter =
          NessieExporter.builder()
              .exportFileSupplier(exportFileSupplier)
              .persist(persist)
              .fullScan(true)
              .build();

      var exportFiles = exporter.exportFileSupplier();

      var exportContext =
          new ExportContext(
              exportFiles,
              exporter,
              ExportMeta.newBuilder()
                  .setNessieVersion(NessieVersion.NESSIE_VERSION)
                  .setCreatedMillisEpoch(System.currentTimeMillis())
                  .setVersion(ExportVersion.V3));

      exportContext.writeCommit(TransferTypes.Commit.newBuilder().build());
      exportContext.writeGeneric(TransferTypes.RelatedObj.newBuilder().build());
      exportContext.writeRef(TransferTypes.Ref.newBuilder().build());

      exportContext.finish();
    }
  }

  @Test
  public void emptyExport(@TempDir Path targetDir, @NessiePersist Persist persist)
      throws Exception {

    repositoryLogic(persist).initialize("main");

    try (ExportFileSupplier exportFileSupplier = prepareExporter(targetDir)) {
      NessieExporter.builder()
          .exportFileSupplier(exportFileSupplier)
          .persist(persist)
          .fullScan(true)
          .build()
          .exportNessieRepository();
    }

    soft.assertThat(readMeta(targetDir))
        .extracting(
            ExportMeta::getCommitCount,
            ExportMeta::getNamedReferencesCount,
            ExportMeta::getCommitsFilesCount,
            ExportMeta::getNamedReferencesFilesCount,
            ExportMeta::getVersion)
        .containsExactly(0L, 1L, 0, 1, ExportVersion.V3);

    List<String> fileNames = listFiles(targetDir);
    soft.assertThat(fileNames)
        .containsExactlyInAnyOrder(
            EXPORT_METADATA, HEADS_AND_FORKS, "named-refs-00000001", "repository-description");
  }

  @Test
  public void manyExportFiles(@TempDir Path targetDir) throws Exception {

    Instant now = Instant.now();
    StoreConfig config =
        StoreConfig.Adjustable.empty().withClock(Clock.fixed(now, ZoneId.of("UTC")));

    Persist exportPersist = mock(Persist.class);
    ReferenceLogic referenceLogic = mock(ReferenceLogic.class);

    int numNamedRefs = 20_000;
    int numCommits = 100_000;

    PagedResult<Reference, String> exportReferences =
        new PagedResult<>() {
          final Iterator<Reference> base =
              IntStream.rangeClosed(1, numNamedRefs)
                  .mapToObj(
                      i ->
                          reference(
                              "refs/heads/branch-" + i,
                              intToObjId(i),
                              false,
                              TimeUnit.NANOSECONDS.toMicros(System.nanoTime()),
                              null))
                  .iterator();

          @Nonnull
          @Override
          public PagingToken tokenForKey(String key) {
            throw new UnsupportedOperationException("Unexpected invocation");
          }

          @Override
          public boolean hasNext() {
            return base.hasNext();
          }

          @Override
          public Reference next() {
            return base.next();
          }
        };
    when(referenceLogic.queryReferences(any())).thenReturn(exportReferences);

    CloseableIterator<Obj> exportScan =
        new CloseableIterator<>() {
          final Iterator<CommitObj> base =
              IntStream.rangeClosed(1, numCommits)
                  .mapToObj(ExportImportTestUtil::toCommitObj)
                  .iterator();

          @Override
          public void close() {}

          @Override
          public boolean hasNext() {
            return base.hasNext();
          }

          @Override
          public Obj next() {
            return base.next();
          }
        };

    when(exportPersist.config()).thenReturn(config);
    when(exportPersist.scanAllObjects(any())).thenReturn(exportScan);
    when(exportPersist.fetchObjsIfExist(any())).thenReturn(new Obj[0]);

    RepositoryLogic repositoryLogic = mock(RepositoryLogic.class);
    when(repositoryLogic.repositoryExists()).thenReturn(true);
    when(repositoryLogic.fetchRepositoryDescription())
        .thenReturn(
            RepositoryDescription.builder()
                .defaultBranchName("main")
                .oldestPossibleCommitTime(now)
                .repositoryCreatedTime(now)
                .build());

    try (ExportFileSupplier exportFileSupplier = prepareExporter(targetDir)) {
      NessieExporter.builder()
          .exportFileSupplier(exportFileSupplier)
          .persist(exportPersist)
          .referenceLogic(referenceLogic)
          .repositoryLogic(repositoryLogic)
          .maxFileSize(128 * 1024)
          .fullScan(true)
          .build()
          .exportNessieRepository();
    }

    int expectedCommitsFileCount = 28;
    List<String> expectedCommitsFileNames =
        IntStream.rangeClosed(1, expectedCommitsFileCount)
            .mapToObj(i -> String.format("commits-%08d", i))
            .collect(Collectors.toList());

    int expectedNamedRefsFileCount = 6;
    List<String> expectedNamesRefsFileNames =
        IntStream.rangeClosed(1, expectedNamedRefsFileCount)
            .mapToObj(i -> String.format("named-refs-%08d", i))
            .collect(Collectors.toList());

    ExportMeta exportMeta = readMeta(targetDir);
    soft.assertThat(exportMeta)
        .extracting(
            ExportMeta::getCommitCount,
            ExportMeta::getNamedReferencesCount,
            ExportMeta::getCommitsFilesCount,
            ExportMeta::getNamedReferencesFilesCount,
            ExportMeta::getVersion)
        .containsExactly(
            (long) numCommits,
            (long) numNamedRefs,
            expectedCommitsFileCount,
            expectedNamedRefsFileCount,
            ExportVersion.V3);
    soft.assertThat(exportMeta.getCommitsFilesList())
        .containsExactlyElementsOf(expectedCommitsFileNames);
    soft.assertThat(exportMeta.getNamedReferencesFilesList())
        .containsExactlyElementsOf(expectedNamesRefsFileNames);

    List<String> fileNames = listFiles(targetDir);
    soft.assertThat(fileNames)
        .hasSize(3 + expectedNamedRefsFileCount + expectedCommitsFileCount)
        .contains(EXPORT_METADATA, HEADS_AND_FORKS, "repository-description")
        .containsAll(expectedNamesRefsFileNames)
        .containsAll(expectedCommitsFileNames);

    // Using Mockito here for the amount of interactions at play is way too slow.

    BitSet createdReferences = new BitSet();
    BitSet createdCommits = new BitSet();

    @SuppressWarnings("resource")
    Persist inmemory = new InmemoryBackend().createFactory().newPersist(config);
    Persist importPersist =
        new Persist() {
          @Nonnull
          @Override
          public String name() {
            return inmemory.name();
          }

          @Nonnull
          @Override
          public StoreConfig config() {
            return inmemory.config();
          }

          @Nonnull
          @Override
          public Reference addReference(@Nonnull Reference reference)
              throws RefAlreadyExistsException {
            return inmemory.addReference(reference);
          }

          @Nonnull
          @Override
          public Reference markReferenceAsDeleted(@Nonnull Reference reference)
              throws RefNotFoundException, RefConditionFailedException {
            return inmemory.markReferenceAsDeleted(reference);
          }

          @Override
          public void purgeReference(@Nonnull Reference reference)
              throws RefNotFoundException, RefConditionFailedException {
            inmemory.purgeReference(reference);
          }

          @Nonnull
          @Override
          public Reference updateReferencePointer(
              @Nonnull Reference reference, @Nonnull ObjId newPointer)
              throws RefNotFoundException, RefConditionFailedException {
            return inmemory.updateReferencePointer(reference, newPointer);
          }

          @Nullable
          @Override
          public Reference fetchReference(@Nonnull String name) {
            if (name.startsWith("refs/heads/branch-")) {
              int refNum = parseInt(name.substring("refs/heads/branch-".length()));
              return reference(
                  name,
                  intToObjId(refNum),
                  false,
                  TimeUnit.NANOSECONDS.toMicros(System.nanoTime()),
                  null);
            }
            return inmemory.fetchReference(name);
          }

          @Nullable
          @Override
          public Reference fetchReferenceForUpdate(@Nonnull String name) {
            if (name.startsWith("refs/heads/branch-")) {
              int refNum = parseInt(name.substring("refs/heads/branch-".length()));
              return reference(
                  name,
                  intToObjId(refNum),
                  false,
                  TimeUnit.NANOSECONDS.toMicros(System.nanoTime()),
                  null);
            }
            return inmemory.fetchReferenceForUpdate(name);
          }

          @Nonnull
          @Override
          public Reference[] fetchReferences(@Nonnull String[] names) {
            return inmemory.fetchReferences(names);
          }

          @Nonnull
          @Override
          public Reference[] fetchReferencesForUpdate(@Nonnull String[] names) {
            return fetchReferences(names);
          }

          @Nonnull
          @Override
          public Obj fetchObj(@Nonnull ObjId id) throws ObjNotFoundException {
            return inmemory.fetchObj(id);
          }

          @Nonnull
          @Override
          public <T extends Obj> T fetchTypedObj(
              @Nonnull ObjId id, ObjType type, @Nonnull Class<T> typeClass)
              throws ObjNotFoundException {
            return inmemory.fetchTypedObj(id, type, typeClass);
          }

          @Nonnull
          @Override
          public ObjType fetchObjType(@Nonnull ObjId id) throws ObjNotFoundException {
            return inmemory.fetchObjType(id);
          }

          @Nonnull
          @Override
          public Obj[] fetchObjs(@Nonnull ObjId[] ids) throws ObjNotFoundException {
            return inmemory.fetchObjs(ids);
          }

          @Nonnull
          @Override
          public <T extends Obj> T[] fetchTypedObjs(
              @Nonnull ObjId[] ids, ObjType type, @Nonnull Class<T> typeClass)
              throws ObjNotFoundException {
            return inmemory.fetchTypedObjs(ids, type, typeClass);
          }

          @Nonnull
          @Override
          public Obj[] fetchObjsIfExist(@Nonnull ObjId[] ids) {
            return inmemory.fetchObjsIfExist(ids);
          }

          @Nonnull
          @Override
          public <T extends Obj> T[] fetchTypedObjsIfExist(
              @Nonnull ObjId[] ids, ObjType type, @Nonnull Class<T> typeClass) {
            return inmemory.fetchTypedObjsIfExist(ids, type, typeClass);
          }

          @Override
          public boolean storeObj(@Nonnull Obj obj, boolean ignoreSoftSizeRestrictions)
              throws ObjTooLargeException {
            return inmemory.storeObj(obj, ignoreSoftSizeRestrictions);
          }

          @Nonnull
          @Override
          public boolean[] storeObjs(@Nonnull Obj[] objs) throws ObjTooLargeException {
            boolean[] r = new boolean[objs.length];
            for (int i = 0; i < objs.length; i++) {
              Obj obj = objs[i];
              if (obj instanceof CommitObj) {
                CommitObj c = (CommitObj) obj;
                if (c.message().startsWith("commit # ")) {
                  int commit = parseInt(c.message().substring("commit # ".length()));
                  assertThat(c)
                      .extracting(
                          CommitObj::id,
                          CommitObj::tail,
                          CommitObj::seq,
                          CommitObj::created,
                          CommitObj::message)
                      .containsExactly(
                          intToObjId(commit),
                          Collections.singletonList(intToObjId(commit - 1)),
                          (long) commit,
                          100L + commit,
                          "commit # " + commit);

                  assertThat(createdCommits.get(commit)).isFalse();
                  createdCommits.set(commit);
                  r[i] = true;
                  continue;
                }
              }
              r[i] = inmemory.storeObj(obj);
            }
            return r;
          }

          @Override
          public void deleteObj(@Nonnull ObjId id) {
            throw new UnsupportedOperationException();
          }

          @Override
          public void deleteObjs(@Nonnull ObjId[] ids) {
            throw new UnsupportedOperationException();
          }

          @Override
          public void upsertObj(@Nonnull Obj obj) {
            throw new UnsupportedOperationException();
          }

          @Override
          public void upsertObjs(@Nonnull Obj[] objs) {
            throw new UnsupportedOperationException();
          }

          @Override
          public boolean deleteWithReferenced(@Nonnull Obj obj) {
            throw new UnsupportedOperationException();
          }

          @Override
          public boolean deleteConditional(@Nonnull UpdateableObj obj) {
            throw new UnsupportedOperationException();
          }

          @Override
          public boolean updateConditional(
              @Nonnull UpdateableObj expected, @Nonnull UpdateableObj newValue) {
            throw new UnsupportedOperationException();
          }

          @Nonnull
          @Override
          public CloseableIterator<Obj> scanAllObjects(@Nonnull Set<ObjType> returnedObjTypes) {
            throw new UnsupportedOperationException();
          }

          @Override
          public void erase() {}
        };

    ReferenceLogic importRefLogic =
        (ReferenceLogic)
            Proxy.newProxyInstance(
                Thread.currentThread().getContextClassLoader(),
                new Class[] {ReferenceLogic.class},
                (proxy, method, args) -> {
                  boolean createReferenceForImport =
                      method.getName().equals("createReferenceForImport");
                  if (method.getName().equals("createReference") || createReferenceForImport) {
                    String name = (String) args[0];
                    ObjId pointer = (ObjId) args[1];
                    assertThat(name).startsWith("refs/heads/branch-");
                    int refNum = parseInt(name.substring("refs/heads/branch-".length()));
                    assertThat(pointer).isEqualTo(intToObjId(refNum));
                    assertThat(createdReferences.get(refNum)).isFalse();
                    createdReferences.set(refNum);
                    long timeMicros =
                        createReferenceForImport
                            ? (Long) args[3]
                            : TimeUnit.NANOSECONDS.toMicros(System.nanoTime());
                    return reference(name, pointer, false, timeMicros, null);
                  }
                  throw new UnsupportedOperationException(method.toString());
                });

    IndexesLogic impIndexesLogic = mock(IndexesLogic.class);
    doNothing().when(impIndexesLogic).completeIndexesInCommitChain(any(), any());

    try (ImportFileSupplier importFileSupplier = prepareImporter(targetDir)) {
      NessieImporter importer =
          NessieImporter.builder()
              .importFileSupplier(importFileSupplier)
              .persist(importPersist)
              .referenceLogic(importRefLogic)
              .indexesLogic(impIndexesLogic)
              .build();
      importer.importNessieRepository();
    }

    soft.assertThat(createdReferences.cardinality()).isEqualTo(numNamedRefs);
    soft.assertThat(createdCommits.cardinality()).isEqualTo(numCommits);
  }

  protected abstract ExportMeta readMeta(Path targetDir) throws IOException;

  protected abstract HeadsAndForks readHeadsAndForks(Path targetDir) throws IOException;

  protected abstract List<String> listFiles(Path targetDir) throws IOException;

  protected abstract ExportFileSupplier prepareExporter(Path targetDir);

  protected abstract ImportFileSupplier prepareImporter(Path targetDir);
}
