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

package com.dremio.nessie.services.rest;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import javax.enterprise.context.RequestScoped;
import javax.inject.Inject;

import org.eclipse.microprofile.metrics.annotation.Metered;
import org.eclipse.microprofile.metrics.annotation.Timed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.nessie.api.ContentsApi;
import com.dremio.nessie.error.NessieConflictException;
import com.dremio.nessie.error.NessieNotFoundException;
import com.dremio.nessie.model.Branch;
import com.dremio.nessie.model.CommitMeta;
import com.dremio.nessie.model.Contents;
import com.dremio.nessie.model.ContentsKey;
import com.dremio.nessie.model.ImmutableCommitMeta;
import com.dremio.nessie.model.MultiContents;
import com.dremio.nessie.model.Operation;
import com.dremio.nessie.model.PutContents;
import com.dremio.nessie.services.config.ServerConfig;
import com.dremio.nessie.versioned.BranchName;
import com.dremio.nessie.versioned.Delete;
import com.dremio.nessie.versioned.Hash;
import com.dremio.nessie.versioned.Key;
import com.dremio.nessie.versioned.Put;
import com.dremio.nessie.versioned.ReferenceConflictException;
import com.dremio.nessie.versioned.ReferenceNotFoundException;
import com.dremio.nessie.versioned.Unchanged;
import com.google.common.collect.ImmutableList;

/**
 * REST endpoint for contents.
 */
@RequestScoped
public class ContentsResource extends BaseResource implements ContentsApi {

  private static final Logger logger = LoggerFactory.getLogger(ContentsResource.class);

  @Inject
  private ServerConfig config;

  @Metered
  @Timed(name = "timed-contents-get")
  @Override
  public Contents getContents(String refName, ContentsKey objectName)
      throws NessieNotFoundException {
    if (refName == null) {
      refName = config.getDefaultBranch();
    }
    Hash ref = getHashOrThrow(refName);
    try {
      Contents obj = store.getValue(ref, toKey(objectName));
      if (obj != null) {
        return obj;
      }
      throw new NessieNotFoundException(refName);
    } catch (ReferenceNotFoundException e) {
      throw new NessieNotFoundException(refName, e);
    }
  }

  @Metered
  @Timed(name = "timed-contents-set")
  @Override
  public void setContents(ContentsKey objectName, String message, PutContents contents)
      throws NessieNotFoundException, NessieConflictException {
    doOps(contents.getBranch(), message, Arrays.asList(Put.of(toKey(objectName), contents.getContents())));
  }

  private void doOps(Branch branch, String message, List<com.dremio.nessie.versioned.Operation<Contents>> operations)
      throws NessieConflictException, NessieNotFoundException {
    try {
      store.commit(
          BranchName.of(branch.getName()),
          Optional.of(Hash.of(branch.getHash())),
          meta(message),
          operations);
    } catch (ReferenceConflictException e) {
      throw new NessieConflictException("Failed to commit data. Provided hash does not match current value.", e);
    } catch (ReferenceNotFoundException e) {
      throw new NessieConflictException("Failed to commit data. Provided ref was not found.", e);
    }
  }

  private CommitMeta meta(String message) {
    return ImmutableCommitMeta.builder()
        .commiter(name())
        .message(message == null ? "" : message)
        .commitTime(System.currentTimeMillis())
        .build();
  }

  private String name() {
    return principal == null ? "" : principal.getName();
  }

  private static Key toKey(ContentsKey key) {
    return Key.of(key.getElements().toArray(new String[key.getElements().size()]));
  }

  @Metered
  @Timed(name = "timed-contents-delete")
  @Override
  public void deleteContents(ContentsKey objectName, String message, Branch branch)
      throws NessieNotFoundException, NessieConflictException {
    doOps(branch, message, Arrays.asList(Delete.of(toKey(objectName))));
  }

  @Metered
  @Timed(name = "timed-contents-multi")
  @Override
  public void commitMultipleOperations(String message, MultiContents operations)
      throws NessieNotFoundException, NessieConflictException {
    List<com.dremio.nessie.versioned.Operation<Contents>> ops = operations.getOperations()
        .stream()
        .map(ContentsResource::toOp)
        .collect(ImmutableList.toImmutableList());
    doOps(operations.getBranch(), message, ops);
  }

  private static com.dremio.nessie.versioned.Operation<Contents> toOp(Operation o) {
    Key key = toKey(o.getKey());
    if (o instanceof Operation.Delete) {
      return Delete.of(key);
    } else if (o instanceof Operation.Put) {
      return Put.of(key, ((Operation.Put)o).getContents());
    } else if (o instanceof Operation.Unchanged) {
      return Unchanged.of(key);
    } else {
      throw new IllegalStateException("Unknown operation " + o);
    }
  }

}
