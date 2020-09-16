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

import static com.dremio.nessie.services.rest.Util.meta;
import static com.dremio.nessie.services.rest.Util.version;

import java.security.Principal;
import java.util.Collections;
import java.util.List;

import javax.enterprise.context.RequestScoped;
import javax.inject.Inject;
import javax.validation.constraints.NotNull;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.core.Response.ResponseBuilder;

import org.eclipse.microprofile.openapi.annotations.Operation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.nessie.api.ContentsApi;
import com.dremio.nessie.api.RefOperationsApi;
import com.dremio.nessie.error.NessieAlreadyExistsException;
import com.dremio.nessie.error.NessieConflictException;
import com.dremio.nessie.error.NessieNotFoundException;
import com.dremio.nessie.model.LogResponse;
import com.dremio.nessie.model.Contents;
import com.dremio.nessie.model.NessieObjectKey;
import com.dremio.nessie.model.ObjectsResponse;
import com.dremio.nessie.model.Reference;
import com.dremio.nessie.services.VersionStoreAdapter;
import com.dremio.nessie.versioned.BranchName;
import com.dremio.nessie.versioned.Put;
import com.dremio.nessie.versioned.ReferenceAlreadyExistsException;
import com.dremio.nessie.versioned.ReferenceConflictException;
import com.dremio.nessie.versioned.ReferenceNotFoundException;

/**
 * REST endpoint for CRUD operations on branches/tables.
 */
@RequestScoped
@Path("objects")
public class TableBranchOperations implements RefOperationsApi, ContentsApi {

  private static final Logger logger = LoggerFactory.getLogger(TableBranchOperations.class);

  private final VersionStoreAdapter backend;
  private final SecurityContext context;

  @Inject
  public TableBranchOperations(@Context SecurityContext context, VersionStoreAdapter backend) {
    this.backend = backend;
    this.context = context;
  }

  @Override
  public Contents getObjectForReference(@NotNull String ref, @NotNull NessieObjectKey objectName, boolean metadata)
      throws NessieNotFoundException {
    try {
      Contents obj = backend.getTableOnReference(ref, objectName);
      if (obj != null) {
        return obj;
      }
    } catch (ReferenceNotFoundException e) {
      throw new NessieNotFoundException(ref, e);
    }

    throw new NessieNotFoundException(ref);
  }

  @Override
  public Response setObject(@NotNull String branchName, @NotNull NessieObjectKey objectName,
      @NotNull String expectedId, String message, Contents table)
      throws NessieNotFoundException, NessieConflictException {
    return singleCommit(branchName, objectName, expectedId, table, message);
  }

  private Response singleCommit(String ref,
      NessieObjectKey key,
      String expectedId,
      Contents table,
      String message) {
     Principal principal = context.getUserPrincipal();
     backend.commit(ref, expectedId, meta(principal, message, ref), Collections.singletonList(Put.of(objectName, table)));
     ResponseBuilder response = post ? Response.created(null) : Response.ok();
     return response.build();
  }

  @Override
  public Response deleteObject(@NotNull String branchName, @NotNull NessieObjectKey objectName,
      @NotNull String expectedId, String message) throws NessieNotFoundException, NessieConflictException {
    return null;
  }

  @Override
  public ObjectsResponse getObjects(@NotNull String refName) throws NessieNotFoundException {
    return null;
  }

  @Override
  public Response commitMultipleOperations(@NotNull String branchName, @NotNull String expectedId, String message,
      List<Operation> operations) throws NessieNotFoundException, NessieConflictException {
    return null;
  }

  @Override
  public List<Reference> getAllReferences() {
    return null;
  }

  @Override
  public Reference getReferenceByName(String refName) throws NessieNotFoundException {
    return null;
  }

  @Override
  public Response createNewReference(String refName, String id)
      throws NessieAlreadyExistsException, NessieNotFoundException {
    return null;
  }

  @Override
  public Response deleteReference(@NotNull String ref, String currentExpectedId)
      throws NessieConflictException, NessieNotFoundException {
    return null;
  }

  @Override
  public Response assignReference(@NotNull String ref, @NotNull String targetId, String currentExpectedId)
      throws NessieNotFoundException, NessieConflictException {
    return null;
  }

  @Override
  public LogResponse getCommitLog(@NotNull String ref) throws NessieNotFoundException {
    return null;
  }

  @Override
  public Response transplantCommitsIntoBranch(@NotNull String branchName, String currentExpectedId, String message,
      List<String> mergeHashes) throws NessieNotFoundException, NessieConflictException {
    return null;
  }

  @Override
  public Response mergeRefIntoBranch(@NotNull String branchName, String currentExpectedId, @NotNull String mergeHash)
      throws NessieNotFoundException, NessieConflictException {
    return null;
  }



}
