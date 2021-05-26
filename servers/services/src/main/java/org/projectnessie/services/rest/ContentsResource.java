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

import java.security.Principal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.enterprise.context.RequestScoped;
import javax.inject.Inject;
import org.projectnessie.api.ContentsApi;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Contents;
import org.projectnessie.model.ContentsKey;
import org.projectnessie.model.ImmutableMultiGetContentsResponse;
import org.projectnessie.model.MultiGetContentsRequest;
import org.projectnessie.model.MultiGetContentsResponse;
import org.projectnessie.model.MultiGetContentsResponse.ContentsWithKey;
import org.projectnessie.services.config.ServerConfig;
import org.projectnessie.versioned.Delete;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.Put;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.VersionStore;

/** REST endpoint for contents. */
@RequestScoped
public class ContentsResource extends BaseResource implements ContentsApi {

  @Inject
  public ContentsResource(
      ServerConfig config,
      Principal principal,
      VersionStore<Contents, CommitMeta, Contents.Type> store) {
    super(config, principal, store);
  }

  @Override
  public Contents getContents(ContentsKey key, String incomingRef) throws NessieNotFoundException {
    Hash ref = getHashOrThrow(incomingRef);
    try {
      Contents obj = getStore().getValue(ref, toKey(key));
      if (obj != null) {
        return obj;
      }
      throw new NessieNotFoundException("Requested contents do not exist for specified reference.");
    } catch (ReferenceNotFoundException e) {
      throw new NessieNotFoundException(
          String.format("Provided reference [%s] does not exist.", incomingRef), e);
    }
  }

  @Override
  public MultiGetContentsResponse getMultipleContents(
      String refName, MultiGetContentsRequest request) throws NessieNotFoundException {
    try {
      Hash ref = getHashOrThrow(refName);
      List<ContentsKey> externalKeys = request.getRequestedKeys();
      List<Key> internalKeys =
          externalKeys.stream().map(ContentsResource::toKey).collect(Collectors.toList());
      List<Optional<Contents>> values = getStore().getValues(ref, internalKeys);
      List<ContentsWithKey> output = new ArrayList<>();

      for (int i = 0; i < externalKeys.size(); i++) {
        final int pos = i;
        values.get(i).ifPresent(v -> output.add(ContentsWithKey.of(externalKeys.get(pos), v)));
      }

      return ImmutableMultiGetContentsResponse.builder().contents(output).build();
    } catch (ReferenceNotFoundException ex) {
      throw new NessieNotFoundException("Unable to find the requested ref.", ex);
    }
  }

  @Override
  public void setContents(
      ContentsKey key, String branch, String hash, String message, Contents contents)
      throws NessieNotFoundException, NessieConflictException {
    CommitMeta meta = CommitMeta.builder().message(message == null ? "" : message).build();
    doOps(branch, hash, meta, Arrays.asList(Put.of(toKey(key), contents)));
  }

  @Override
  public void deleteContents(ContentsKey key, String branch, String hash, String message)
      throws NessieNotFoundException, NessieConflictException {
    CommitMeta meta = CommitMeta.builder().message(message == null ? "" : message).build();
    doOps(branch, hash, meta, Arrays.asList(Delete.of(toKey(key))));
  }

  static Key toKey(ContentsKey key) {
    return Key.of(key.getElements().toArray(new String[key.getElements().size()]));
  }
}
