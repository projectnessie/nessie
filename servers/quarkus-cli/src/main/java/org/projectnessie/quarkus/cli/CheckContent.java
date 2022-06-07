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

import com.google.protobuf.ByteString;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.projectnessie.versioned.GetNamedRefsParams;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.ReferenceInfo;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.StoreWorker;
import org.projectnessie.versioned.persist.adapter.ContentAndState;
import org.projectnessie.versioned.persist.adapter.KeyFilterPredicate;
import org.projectnessie.versioned.persist.adapter.KeyListEntry;
import picocli.CommandLine;

@CommandLine.Command(
    name = "check-content",
    mixinStandardHelpOptions = true,
    description = "Check content readability of active keys.")
public class CheckContent extends BaseCommand {

  @CommandLine.Option(
      names = {"-s", "--separator"},
      defaultValue = ": ",
      description = "Output field separator.")
  private String separator;

  @CommandLine.Option(
      names = {"-k", "--key"},
      description =
          "Content key element (zero or more). If not set, all current keys will be checked.")
  private List<String> contentKey;

  @CommandLine.Option(
      names = {"-c", "--show-content"},
      description = "Print content details for each valid key.")
  private boolean showContent;

  @CommandLine.Option(
      names = {"-B", "--batch"},
      defaultValue = "25",
      description = "The max number of keys to load at the same time.")
  private int batchSize;

  @CommandLine.Option(
      names = {"-W", "--worker-class"},
      defaultValue = "org.projectnessie.server.store.TableCommitMetaStoreWorker",
      description = "The class name of the Nessie Store Worker (internal).")
  private String workerClass;

  @CommandLine.Option(
      names = {"-r", "--ref"},
      description = "Reference name to use (default branch, if not set).")
  private String ref;

  @CommandLine.Option(
      names = {"-H", "--hash"},
      description = "Commit hash to use (defaults to the HEAD of the specified reference).")
  private String hash;

  @CommandLine.Option(
      names = {"-v", "--verbose"},
      description = "Print extra error information to STDERR.")
  private boolean verbose;

  @CommandLine.Option(
      names = {"-E", "--error-only"},
      description = "Print status information only for keys with errors")
  private boolean errorOnly;

  private final AtomicInteger errorDetected = new AtomicInteger();

  @Override
  public Integer call() throws Exception {
    warnOnInMemory();

    StoreWorker<?, ?, ?> worker =
        (StoreWorker<?, ?, ?>)
            getClass()
                .getClassLoader()
                .loadClass(workerClass)
                .getDeclaredConstructor()
                .newInstance();

    Hash hash = hash();
    if (contentKey != null && !contentKey.isEmpty()) {
      check(hash, List.of(Key.of(contentKey)), worker);
    } else {
      List<Key> batch = new ArrayList<>(batchSize);
      try (Stream<KeyListEntry> keys = databaseAdapter.keys(hash, KeyFilterPredicate.ALLOW_ALL)) {
        keys.forEach(
            keyListEntry -> {
              batch.add(keyListEntry.getKey());

              if (batch.size() >= batchSize) {
                check(hash, batch, worker);
                batch.clear();
              }
            });

        check(hash, batch, worker); // check remaining keys
      }
    }

    return errorDetected.get() == 0 ? 0 : 2;
  }

  private Hash hash() throws ReferenceNotFoundException {
    if (hash != null) {
      return Hash.of(hash);
    }

    String effectiveRef = ref;
    if (effectiveRef == null) {
      effectiveRef = serverConfig.getDefaultBranch();
    }

    ReferenceInfo<ByteString> main =
        databaseAdapter.namedRef(effectiveRef, GetNamedRefsParams.DEFAULT);
    return main.getHash();
  }

  private void check(Hash hash, List<Key> keys, StoreWorker<?, ?, ?> worker) {
    Map<Key, ContentAndState<ByteString>> values;
    try {
      values = databaseAdapter.values(hash, keys, KeyFilterPredicate.ALLOW_ALL);
    } catch (Exception e) {
      keys.forEach(k -> report(k, e));
      return;
    }

    keys.forEach(
        k -> {
          if (values.get(k) == null) {
            report(k, "Missing content", null);
          }
        });

    values.forEach(
        (k, contentAndState) -> {
          try {
            Object value =
                worker.valueFromStore(
                    contentAndState.getRefState(), contentAndState::getGlobalState);
            report(k, null, value);
          } catch (Exception e) {
            report(k, e);
          }
        });
  }

  private void report(Key key, Throwable error) {
    if (verbose) {
      spec.commandLine().getErr().printf("Error for key %s%n", key);
      error.printStackTrace(spec.commandLine().getErr());
    }

    StringBuilder msg = new StringBuilder(error.toString());
    while ((error = error.getCause()) != null) {
      msg.append("; Caused by: ");
      msg.append(error);
    }

    report(key, msg.toString(), null);
  }

  private void report(Key key, String error, Object content) {
    if (error != null) {
      errorDetected.incrementAndGet();
    }

    if (error == null && errorOnly) {
      return;
    }

    PrintWriter out = spec.commandLine().getOut();
    out.printf(error == null ? "OK" : "ERROR");
    out.print(separator);
    out.print(key);
    out.print(separator);
    out.print(error != null ? error : (showContent ? String.valueOf(content) : ""));
    out.println();
  }
}
