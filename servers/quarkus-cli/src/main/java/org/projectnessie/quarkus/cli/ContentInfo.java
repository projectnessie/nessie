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
package org.projectnessie.quarkus.cli;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.projectnessie.versioned.persist.adapter.spi.DatabaseAdapterUtil.takeUntilExcludeLast;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.errorprone.annotations.MustBeClosed;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.versioned.DetachedRef;
import org.projectnessie.versioned.GetNamedRefsParams;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.ReferenceInfo;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.persist.adapter.CommitLogEntry;
import org.projectnessie.versioned.persist.adapter.KeyFilterPredicate;
import org.projectnessie.versioned.persist.adapter.KeyListEntry;
import org.projectnessie.versioned.persist.adapter.KeyWithBytes;
import org.projectnessie.versioned.store.DefaultStoreWorker;
import picocli.CommandLine;

@CommandLine.Command(
    name = "content-info",
    mixinStandardHelpOptions = true,
    description = "Get information about how content is stored for a given set of keys.")
public class ContentInfo extends BaseCommand {

  @CommandLine.Option(
      names = {"-o", "--output"},
      description =
          "JSON output file name or '-' for STDOUT. If not set, per-key status is not reported.")
  private String outputSpec;

  @CommandLine.Option(
      names = {"-k", "--key-element"},
      description =
          "Elements or a specific content key to check (zero or more). If not set, all current keys will be reported.")
  private List<String> keyElements;

  @CommandLine.Option(
      names = {"-B", "--batch"},
      defaultValue = "1000",
      description = "The maximum number of keys to process at the same time.")
  private int batchSize;

  @CommandLine.Option(
      names = {"-r", "--ref"},
      description = "Reference name to use (all branches, if not set).")
  private String ref;

  @CommandLine.Option(
      names = {"-H", "--hash"},
      description = "Commit hash to use (defaults to the HEAD of the specified reference).")
  private String hash;

  @CommandLine.Option(
      names = {"-s", "--summary"},
      description = "Print a summary of results to STDOUT (irrespective of the --output option).")
  private boolean summary;

  private final AtomicInteger keysProcessed = new AtomicInteger();
  private final AtomicInteger activeGlobalStateEntries = new AtomicInteger();
  private final AtomicInteger missingContentFound = new AtomicInteger();

  @Override
  protected Integer callWithDatabaseAdapter() throws Exception {
    warnOnInMemory();

    if (outputSpec != null) {
      if ("-".equals(outputSpec)) {
        check(spec.commandLine().getOut());
        spec.commandLine().getOut().println();
      } else {
        try (PrintWriter out = new PrintWriter(outputSpec, UTF_8)) {
          check(out);
        }
      }
    } else {
      check(new PrintWriter(OutputStream.nullOutputStream(), false, UTF_8));
    }

    if (summary) {
      spec.commandLine()
          .getOut()
          .printf(
              "Processed %d keys: %d entries have global state; %d missing entries.%n",
              keysProcessed.get(), activeGlobalStateEntries.get(), missingContentFound.get());
    }

    return missingContentFound.get() > 0 ? EXIT_CODE_CONTENT_ERROR : 0;
  }

  private void check(PrintWriter out) throws Exception {
    // Note: Do not use try-with-resources to avoid closing the output. The caller takes care of
    // that.
    JsonGenerator generator =
        new ObjectMapper()
            .configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
            .getFactory()
            .createGenerator(out);

    generator.writeStartArray();

    check(generator);

    generator.writeEndArray();
    generator.flush();
  }

  private void check(JsonGenerator generator) throws Exception {
    try (Stream<ReferenceInfo<ByteString>> heads = heads()) {
      heads.forEach(
          head -> {
            try {
              check(generator, head);
            } catch (Exception e) {
              throw new IllegalStateException(e);
            }
          });
    }
  }

  private void check(JsonGenerator generator, ReferenceInfo<ByteString> head) throws Exception {
    if (keyElements != null && !keyElements.isEmpty()) {
      check(head, Collections.singleton(ContentKey.of(keyElements)), generator);
    } else {
      Set<ContentKey> batch = new HashSet<>(batchSize);
      try (Stream<KeyListEntry> keys =
          databaseAdapter.keys(head.getHash(), KeyFilterPredicate.ALLOW_ALL)) {
        keys.forEach(
            keyListEntry -> {
              batch.add(keyListEntry.getKey());

              if (batch.size() >= batchSize) {
                check(head, batch, generator);
                batch.clear();
              }
            });
      }

      if (!batch.isEmpty()) {
        check(head, batch, generator); // check remaining keys
      }
    }
  }

  @MustBeClosed
  private Stream<ReferenceInfo<ByteString>> heads() throws ReferenceNotFoundException {
    if (hash != null) {
      return Stream.of(ReferenceInfo.of(Hash.of(hash), DetachedRef.INSTANCE));
    }

    if (ref != null) {
      ReferenceInfo<ByteString> refInfo =
          databaseAdapter.namedRef(serverConfig.getDefaultBranch(), GetNamedRefsParams.DEFAULT);
      return Stream.of(refInfo);
    }

    return databaseAdapter.namedRefs(GetNamedRefsParams.DEFAULT);
  }

  private void check(
      ReferenceInfo<ByteString> head, Set<ContentKey> keys, JsonGenerator generator) {
    try {
      AtomicInteger distanceFromHead = new AtomicInteger();
      try (Stream<CommitLogEntry> commitLog = databaseAdapter.commitLog(head.getHash());
          Stream<CommitLogEntry> filteredLog =
              takeUntilExcludeLast(commitLog, e -> keys.isEmpty())) {
        filteredLog.forEach(
            commitLogEntry -> {
              commitLogEntry
                  .getPuts()
                  .forEach(
                      keyWithBytes -> {
                        if (keys.remove(keyWithBytes.getKey())) {
                          report(
                              generator,
                              head.getNamedRef().getName(),
                              keyWithBytes.getKey(),
                              keyWithBytes,
                              commitLogEntry,
                              distanceFromHead.get(),
                              null);
                        }
                      });
              distanceFromHead.incrementAndGet();
            });
      }
    } catch (Exception e) {
      keys.forEach(k -> report(generator, head.getNamedRef().getName(), k, null, null, -1, e));
      return;
    }

    missingContentFound.addAndGet(keys.size());
    keys.forEach(
        k ->
            report(
                generator,
                head.getNamedRef().getName(),
                k,
                null,
                null,
                -1,
                new IllegalArgumentException("Missing content")));
  }

  private void report(
      JsonGenerator generator,
      String referenceName,
      ContentKey key,
      KeyWithBytes value,
      CommitLogEntry entry,
      long distanceFromHead,
      Throwable error) {
    keysProcessed.incrementAndGet();

    ImmutableContentInfoEntry.Builder builder = ImmutableContentInfoEntry.builder();
    builder.reference(referenceName);
    builder.key(ContentKey.of(key.getElements()));
    builder.distanceFromHead(distanceFromHead);

    if (entry != null) {
      builder.distanceFromRoot(entry.getCommitSeq());
      builder.hash(entry.getHash().asString());
    }

    Content.Type type = Content.Type.UNKNOWN;
    String storageModel = "UNKNOWN";

    if (error == null && entry != null) {
      try {
        type = DefaultStoreWorker.instance().getType(value.getPayload(), value.getValue());
        boolean globalState =
            DefaultStoreWorker.instance().requiresGlobalState(value.getPayload(), value.getValue());
        if (globalState) {
          activeGlobalStateEntries.incrementAndGet();
          storageModel = "GLOBAL_STATE";
        } else {
          storageModel = "ON_REF_STATE";
        }
      } catch (Exception ex) {
        error = ex;
      }
    }

    builder.type(type);
    builder.storageModel(storageModel);

    if (error != null) {
      builder.errorMessage(error.getMessage());

      try (StringWriter wr = new StringWriter();
          PrintWriter pw = new PrintWriter(wr)) {
        error.printStackTrace(pw);
        pw.flush();
        builder.exceptionStackTrace(wr.toString());
      } catch (Exception e) {
        throw new AssertionError(e);
      }
    }

    try {
      generator.writeObject(builder.build());

      // Write a new line after each object to make monitoring I/O more pleasant and predictable
      Object out = generator.getOutputTarget();
      if (out instanceof PrintWriter) {
        ((PrintWriter) out).println();
      }
      generator.flush();

    } catch (Exception e) {
      throw new AssertionError(e);
    }
  }
}
