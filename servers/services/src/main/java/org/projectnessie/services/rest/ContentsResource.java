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

import javax.enterprise.context.RequestScoped;
import javax.inject.Inject;

import org.projectnessie.api.rest.ContentsRestApi;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Contents;
import org.projectnessie.model.ContentsKey;
import org.projectnessie.model.MultiGetContentsRequest;
import org.projectnessie.model.MultiGetContentsResponse;
import org.projectnessie.services.config.ServerConfig;
import org.projectnessie.services.impl.ContentsApiImpl;
import org.projectnessie.versioned.VersionStore;

/**
 * REST endpoint for contents.
 */
@RequestScoped
public class ContentsResource implements ContentsRestApi {

  /**
   * Delegate to (do not extend) the implementation for better code-coverage.
   */
  private final ContentsApiImpl delegate;

  @Inject
  public ContentsResource(ServerConfig config, Principal principal,
      VersionStore<Contents, CommitMeta> store) {
    delegate = new ContentsApiImpl(config, principal, store);
  }

  @Override
  public Contents getContents(ContentsKey key, String incomingRef) throws NessieNotFoundException {
    return delegate.getContents(key, incomingRef);
  }

  @Override
  public MultiGetContentsResponse getMultipleContents(String refName,
      MultiGetContentsRequest request) throws NessieNotFoundException {
    return delegate.getMultipleContents(refName, request);
  }

  @Override
  public void setContents(ContentsKey key, String branch, String hash, String message,
      Contents contents) throws NessieNotFoundException, NessieConflictException {
    delegate.setContents(key, branch, hash, message, contents);
  }

  @Override
  public void deleteContents(ContentsKey key, String branch, String hash, String message)
      throws NessieNotFoundException, NessieConflictException {
    delegate.deleteContents(key, branch, hash, message);
  }
}
