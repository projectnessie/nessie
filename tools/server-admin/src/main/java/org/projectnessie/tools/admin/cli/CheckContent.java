/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.tools.admin.cli;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.projectnessie.versioned.storage.common.logic.Logics.commitLogic;
import static org.projectnessie.versioned.storage.common.logic.Logics.indexesLogic;
import static org.projectnessie.versioned.storage.versionstore.RefMapping.hashNotFound;
import static org.projectnessie.versioned.storage.versionstore.RefMapping.objectNotFound;
import static org.projectnessie.versioned.storage.versionstore.TypeMapping.hashToObjId;
import static org.projectnessie.versioned.storage.versionstore.TypeMapping.storeKeyToKey;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.storage.common.exceptions.ObjNotFoundException;
import org.projectnessie.versioned.storage.common.indexes.StoreIndex;
import org.projectnessie.versioned.storage.common.objtypes.CommitObj;
import org.projectnessie.versioned.storage.common.objtypes.CommitOp;
import org.projectnessie.versioned.storage.versionstore.ContentMapping;
import picocli.CommandLine;

@CommandLine.Command(
    name = "check-content",
    mixinStandardHelpOptions = true,
    description = "Check content readability of active keys.")
public class CheckContent extends BaseCommand {

  @CommandLine.Option(
      names = {"-o", "--output"},
      description =
          "JSON output file name or '-' for STDOUT. If not set, per-key status is not reported.")
  private String outputSpec;

  @CommandLine.Option(
      names = {"-k", "--key-element"},
      description =
          "Elements or a specific content key to check (zero or more). If not set, all current keys will be checked.")
  private List<String> keyElements;

  @CommandLine.Option(
      names = {"-c", "--show-content"},
      description = "Include content for each valid key in the output.")
  private boolean showContent;

  @CommandLine.Option(
      names = {"-B", "--batch"},
      defaultValue = "25",
      description = {
        "The max number of keys to load at the same time.",
        "If an error occurs while loading or parsing the values for a single key, the error "
            + "will be propagated to all keys processed in the same batch. In such a case, rerun "
            + "the check for the affected keys with a batch size of 1."
      })
  private int batchSize;

  @CommandLine.Option(
      names = {"-r", "--ref"},
      description = "Reference name to use (default branch, if not set).")
  private String ref;

  @CommandLine.Option(
      names = {"-H", "--hash"},
      description = "Commit hash to use (defaults to the HEAD of the specified reference).")
  private String hash;

  @CommandLine.Option(
      names = {"-s", "--summary"},
      description = "Print a summary of results to STDOUT (irrespective of the --output option).")
  private boolean summary;

  @CommandLine.Option(
      names = {"-E", "--error-only"},
      description = "Produce JSON only for keys with errors.")
  private boolean errorOnly;

  private final AtomicInteger keysProcessed = new AtomicInteger();
  private final AtomicInteger errorDetected = new AtomicInteger();

  private JsonGenerator generator;
  private StoreIndex<CommitOp> index;

  @Override
  public Integer call() throws Exception {
    return check();
  }

  private Integer check() throws Exception {
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
          .printf("Detected %d errors in %d keys.%n", errorDetected.get(), keysProcessed.get());
    }

    return errorDetected.get() == 0 ? 0 : EXIT_CODE_CONTENT_ERROR;
  }

  private void check(PrintWriter out) throws Exception {
    // Note: Do not use try-with-resources to avoid closing the output. The caller takes care of
    // that.
    generator =
        new ObjectMapper()
            .configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
            .getFactory()
            .createGenerator(out);

    generator.writeStartArray();

    Hash hash = hash(this.hash, ref);
    if (keyElements != null && !keyElements.isEmpty()) {
      check(hash, List.of(ContentKey.of(keyElements)));
    } else {
      iterateKeys(hash);
    }

    generator.writeEndArray();
    generator.flush();
  }

  private void check(Hash hash, List<ContentKey> keys) {
    Map<ContentKey, Content> values;
    try {
      values = fetchValues(hash, keys);
    } catch (Exception e) {
      keys.forEach(k -> report(k, e, null));
      return;
    }

    keys.forEach(
        k -> {
          if (values.get(k) == null) {
            report(k, new IllegalArgumentException("Missing content"), null);
          }
        });

    values.forEach(
        (k, value) -> {
          try {
            report(k, null, value);
          } catch (Exception e) {
            report(k, e, null);
          }
        });
  }

  private void report(ContentKey key, Throwable error, Object content) {
    keysProcessed.incrementAndGet();
    if (error != null) {
      errorDetected.incrementAndGet();
    }

    if (error == null && errorOnly) {
      return;
    }

    ImmutableCheckContentEntry.Builder builder = ImmutableCheckContentEntry.builder();
    builder.key(key);
    builder.status(error == null ? "OK" : "ERROR");

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

    if (showContent && (content instanceof Content)) {
      builder.content((Content) content);
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

  private void iterateKeys(Hash hash) throws ReferenceNotFoundException {
    List<ContentKey> batch = new ArrayList<>(batchSize);
    index(hash)
        .iterator(null, null, true)
        .forEachRemaining(
            indexElement -> {
              if (indexElement.content().action().exists()) {
                ContentKey key = storeKeyToKey(indexElement.key());
                batch.add(key);
                if (batch.size() >= batchSize) {
                  check(hash, batch);
                  batch.clear();
                }
              }
            });
    check(hash, batch); // check remaining keys
  }

  private Map<ContentKey, Content> fetchValues(Hash hash, List<ContentKey> keys)
      throws ReferenceNotFoundException {
    StoreIndex<CommitOp> index = index(hash);
    ContentMapping contentMapping = new ContentMapping(persist);
    try {
      return contentMapping.fetchContents(index, keys);
    } catch (ObjNotFoundException e) {
      throw objectNotFound(e);
    }
  }

  private StoreIndex<CommitOp> index(Hash hash) throws ReferenceNotFoundException {
    if (index == null) {
      try {
        CommitObj c = commitLogic(persist).fetchCommit(hashToObjId(hash));
        index = indexesLogic(persist).buildCompleteIndexOrEmpty(c);
      } catch (ObjNotFoundException e) {
        throw hashNotFound(hash);
      }
    }
    return index;
  }
}
