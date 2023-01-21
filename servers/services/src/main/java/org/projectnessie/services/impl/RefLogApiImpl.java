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

import static org.projectnessie.services.cel.CELUtil.CONTAINER;
import static org.projectnessie.services.cel.CELUtil.REFLOG_DECLARATIONS;
import static org.projectnessie.services.cel.CELUtil.REFLOG_TYPES;
import static org.projectnessie.services.cel.CELUtil.SCRIPT_HOST;
import static org.projectnessie.services.cel.CELUtil.VAR_REFLOG;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import java.security.Principal;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.projectnessie.cel.tools.Script;
import org.projectnessie.cel.tools.ScriptException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.error.NessieRefLogNotFoundException;
import org.projectnessie.model.ImmutableRefLogResponse;
import org.projectnessie.model.ImmutableRefLogResponseEntry;
import org.projectnessie.model.RefLogResponse;
import org.projectnessie.services.authz.Authorizer;
import org.projectnessie.services.config.ServerConfig;
import org.projectnessie.services.spi.RefLogService;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.RefLogDetails;
import org.projectnessie.versioned.RefLogNotFoundException;
import org.projectnessie.versioned.VersionStore;

public class RefLogApiImpl extends BaseApiImpl implements RefLogService {

  private static final int MAX_REF_LOG_ENTRIES = 250;

  public RefLogApiImpl(
      ServerConfig config,
      VersionStore store,
      Authorizer authorizer,
      Supplier<Principal> principal) {
    super(config, store, authorizer, principal);
  }

  @Override
  public RefLogResponse getRefLog(
      String startHashString,
      String endHashString,
      String filter,
      Integer maxRecords,
      String pageToken)
      throws NessieNotFoundException {
    startAccessCheck().canViewRefLog().checkAndThrow();

    int max = Math.min(maxRecords != null ? maxRecords : MAX_REF_LOG_ENTRIES, MAX_REF_LOG_ENTRIES);

    Hash endHash = null;
    if (endHashString != null) {
      endHash = Hash.of(Objects.requireNonNull(endHashString));
    }
    Hash endRef = null == pageToken ? endHash : Hash.of(pageToken);

    try (Stream<RefLogDetails> entries = getStore().getRefLog(endRef)) {
      Stream<RefLogResponse.RefLogResponseEntry> logEntries =
          entries.map(
              entry -> {
                ImmutableRefLogResponseEntry.Builder logEntry =
                    RefLogResponse.RefLogResponseEntry.builder();
                logEntry
                    .refLogId(entry.getRefLogId().asString())
                    .refName(entry.getRefName())
                    .refType(entry.getRefType())
                    .commitHash(entry.getCommitHash().asString())
                    .operation(entry.getOperation())
                    .operationTime(entry.getOperationTime())
                    .parentRefLogId(entry.getParentRefLogId().asString());
                entry.getSourceHashes().forEach(hash -> logEntry.addSourceHashes(hash.asString()));
                return logEntry.build();
              });

      logEntries =
          StreamSupport.stream(
              StreamUtil.takeUntilIncl(
                  logEntries.spliterator(), x -> Objects.equals(x.getRefLogId(), startHashString)),
              false);

      List<RefLogResponse.RefLogResponseEntry> items =
          filterRefLog(logEntries, filter).limit(max + 1).collect(Collectors.toList());

      if (items.size() == max + 1) {
        return ImmutableRefLogResponse.builder()
            .addAllLogEntries(items.subList(0, max))
            .isHasMore(true)
            .token(items.get(max).getRefLogId())
            .build();
      }
      return ImmutableRefLogResponse.builder().addAllLogEntries(items).build();
    } catch (RefLogNotFoundException e) {
      throw new NessieRefLogNotFoundException(e.getMessage(), e);
    }
  }

  /**
   * Applies different filters to the {@link Stream} of reflog entries based on the filter.
   *
   * @param logEntries The reflog that different filters will be applied to
   * @param filter The filter to filter by
   * @return A potentially filtered {@link Stream} of reflog entries based on the filter
   */
  private static Stream<RefLogResponse.RefLogResponseEntry> filterRefLog(
      Stream<RefLogResponse.RefLogResponseEntry> logEntries, String filter) {
    if (Strings.isNullOrEmpty(filter)) {
      return logEntries;
    }

    final Script script;
    try {
      script =
          SCRIPT_HOST
              .buildScript(filter)
              .withContainer(CONTAINER)
              .withDeclarations(REFLOG_DECLARATIONS)
              .withTypes(REFLOG_TYPES)
              .build();
    } catch (ScriptException e) {
      throw new IllegalArgumentException(e);
    }
    return logEntries.filter(
        logEntry -> {
          try {
            return script.execute(Boolean.class, ImmutableMap.of(VAR_REFLOG, logEntry));
          } catch (ScriptException e) {
            throw new RuntimeException(e);
          }
        });
  }
}
