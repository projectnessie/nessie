/*
 * Copyright (C) 2023 Dremio
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
package org.projectnessie.versioned.storage.versionstore;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.projectnessie.versioned.storage.common.logic.CommitRetry.commitRetry;
import static org.projectnessie.versioned.storage.common.logic.Logics.commitLogic;
import static org.projectnessie.versioned.storage.common.logic.Logics.indexesLogic;
import static org.projectnessie.versioned.storage.common.logic.Logics.referenceLogic;
import static org.projectnessie.versioned.storage.common.logic.Logics.stringLogic;
import static org.projectnessie.versioned.storage.common.objtypes.StandardObjType.STRING;
import static org.projectnessie.versioned.storage.common.util.Ser.SHARED_OBJECT_MAPPER;
import static org.projectnessie.versioned.storage.versionstore.RefMapping.referenceConflictException;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.projectnessie.model.RepositoryConfig;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.ReferenceRetryFailureException;
import org.projectnessie.versioned.storage.common.exceptions.CommitConflictException;
import org.projectnessie.versioned.storage.common.exceptions.CommitWrappedException;
import org.projectnessie.versioned.storage.common.exceptions.ObjNotFoundException;
import org.projectnessie.versioned.storage.common.exceptions.RefAlreadyExistsException;
import org.projectnessie.versioned.storage.common.exceptions.RefConditionFailedException;
import org.projectnessie.versioned.storage.common.exceptions.RefNotFoundException;
import org.projectnessie.versioned.storage.common.exceptions.RetryTimeoutException;
import org.projectnessie.versioned.storage.common.indexes.StoreIndex;
import org.projectnessie.versioned.storage.common.indexes.StoreIndexElement;
import org.projectnessie.versioned.storage.common.indexes.StoreKey;
import org.projectnessie.versioned.storage.common.logic.CommitRetry;
import org.projectnessie.versioned.storage.common.logic.IndexesLogic;
import org.projectnessie.versioned.storage.common.logic.ReferenceLogic;
import org.projectnessie.versioned.storage.common.logic.StringLogic;
import org.projectnessie.versioned.storage.common.logic.StringLogic.StringValue;
import org.projectnessie.versioned.storage.common.objtypes.CommitObj;
import org.projectnessie.versioned.storage.common.objtypes.CommitOp;
import org.projectnessie.versioned.storage.common.objtypes.StringObj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.common.persist.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class RepositoryConfigBackend {
  private static final Logger LOGGER = LoggerFactory.getLogger(RepositoryConfigBackend.class);

  static final String REPO_CONFIG_REF = "nessie/configs";

  RepositoryConfigBackend(Persist persist) {
    this.persist = persist;
  }

  final Persist persist;

  public List<RepositoryConfig> getConfigs(Set<RepositoryConfig.Type> repositoryConfigTypes) {
    try {
      Persist p = persist;
      Reference reference = configsRef(false);
      IndexesLogic indexesLogic = indexesLogic(p);
      CommitObj head = commitLogic(p).headCommit(reference);
      StoreIndex<CommitOp> index = indexesLogic.buildCompleteIndexOrEmpty(head);

      StringObj[] objs =
          p.fetchTypedObjs(
              repositoryConfigTypes.stream()
                  .map(RepositoryConfigBackend::repositoryConfigTypeToStoreKey)
                  .map(storeKey -> valueObjIdFromIndex(index, storeKey))
                  .toArray(ObjId[]::new),
              STRING,
              StringObj.class);

      StringLogic stringLogic = stringLogic(p);

      return Arrays.stream(objs)
          .filter(Objects::nonNull)
          .map(stringLogic::fetchString)
          .map(RepositoryConfigBackend::deserialize)
          .collect(Collectors.toList());
    } catch (ObjNotFoundException | RetryTimeoutException e) {
      throw new RuntimeException(e);
    }
  }

  public RepositoryConfig updateConfig(RepositoryConfig repositoryConfig)
      throws ReferenceConflictException {
    RepositoryConfig.Type type = repositoryConfig.getType();
    StoreKey storeKey = repositoryConfigTypeToStoreKey(type);

    try {
      return commitRetry(
          persist,
          (p, retryState) -> {
            Reference reference;
            try {
              reference = configsRef(true);
            } catch (RetryTimeoutException ex) {
              throw new CommitWrappedException(new CommitRetry.RetryException(Optional.empty()));
            }

            try {
              StringValue existing =
                  stringLogic(persist)
                      .updateStringOnRef(
                          reference,
                          storeKey,
                          b -> b.message("Update config " + type.name()),
                          "application/json",
                          serialize(repositoryConfig));
              return existing != null ? deserialize(existing) : null;
            } catch (ObjNotFoundException | RefNotFoundException | RefConditionFailedException e) {
              throw new CommitWrappedException(e);
            }
          });
    } catch (CommitConflictException e) {
      throw referenceConflictException(e);
    } catch (CommitWrappedException e) {
      Throwable c = e.getCause();
      if (c instanceof ReferenceConflictException) {
        throw (ReferenceConflictException) c;
      }
      if (c instanceof RuntimeException) {
        throw (RuntimeException) c;
      }
      throw new RuntimeException(c);
    } catch (RetryTimeoutException e) {
      long millis = NANOSECONDS.toMillis(e.getTimeNanos());
      String msg =
          format(
              "The repository config update operation could not be performed after %d retries within the configured commit timeout after %d milliseconds",
              e.getRetry(), millis);
      LOGGER.warn("Operation timeout: {}", msg);
      throw new ReferenceRetryFailureException(msg, e.getRetry(), millis);
    }
  }

  private static byte[] serialize(RepositoryConfig repositoryConfig) {
    try {
      return SHARED_OBJECT_MAPPER.writeValueAsBytes(repositoryConfig);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static RepositoryConfig deserialize(StringValue value) {
    try {
      return SHARED_OBJECT_MAPPER.readValue(value.completeValue(), RepositoryConfig.class);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /** Retrieves the configs-reference, creates the reference, if it does not exist. */
  private Reference configsRef(boolean bypassCache) throws RetryTimeoutException {
    ReferenceLogic referenceLogic = referenceLogic(persist);
    Reference reference;
    try {
      reference =
          bypassCache
              ? referenceLogic.getReferenceForUpdate(REPO_CONFIG_REF)
              : referenceLogic.getReference(REPO_CONFIG_REF);
    } catch (RefNotFoundException e) {
      try {
        reference = referenceLogic.createReference(REPO_CONFIG_REF, ObjId.EMPTY_OBJ_ID, null);
      } catch (RefAlreadyExistsException ex) {
        reference = ex.reference();
        if (reference == null) {
          throw new RuntimeException(e);
        }
      }
    }
    return reference;
  }

  private static ObjId valueObjIdFromIndex(StoreIndex<CommitOp> index, StoreKey storeKey) {
    StoreIndexElement<CommitOp> existing = index.get(storeKey);
    if (existing == null) {
      return null;
    }
    if (!existing.content().action().exists()) {
      return null;
    }
    return existing.content().value();
  }

  private static StoreKey repositoryConfigTypeToStoreKey(RepositoryConfig.Type type) {
    return StoreKey.key("configs", type.name());
  }
}
