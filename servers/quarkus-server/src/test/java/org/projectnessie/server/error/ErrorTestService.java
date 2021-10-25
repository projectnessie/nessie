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
package org.projectnessie.server.error;

import javax.enterprise.context.RequestScoped;
import javax.validation.ConstraintDeclarationException;
import javax.validation.ConstraintDefinitionException;
import javax.validation.GroupDefinitionException;
import javax.validation.Valid;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import org.mockito.Mockito;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.error.NessieReferenceNotFoundException;
import org.projectnessie.versioned.BackendLimitExceededException;
import org.projectnessie.versioned.StringStoreWorker;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.store.PersistVersionStore;

/** REST service used to generate a bunch of violations for {@link TestNessieError}. */
@RequestScoped
@Path("/nessieErrorTest")
@Consumes(MediaType.WILDCARD)
@Produces(MediaType.APPLICATION_JSON)
public class ErrorTestService {

  @Path("nullParameterQueryGet")
  @GET
  public String nullParameterQueryGet(@NotNull @QueryParam("hash") String hash) {
    return "oh oh";
  }

  @Path("nullParameterQueryPost")
  @POST
  public String nullParameterQueryPost(@NotNull @QueryParam("hash") String hash) {
    return "oh oh";
  }

  @Path("emptyParameterQueryGet")
  @GET
  public String emptyParameterQueryGet(@NotEmpty @QueryParam("hash") String hash) {
    return "oh oh";
  }

  @Path("blankParameterQueryGet")
  @GET
  public String blankParameterQueryGet(@NotBlank @QueryParam("hash") String hash) {
    return "oh oh";
  }

  @Path("nessieNotFound")
  @GET
  public String nessieNotFound() throws NessieNotFoundException {
    throw new NessieReferenceNotFoundException(
        "not-there-message", new Exception("not-there-exception"));
  }

  @Path("basicEntity")
  @PUT
  @Consumes(MediaType.APPLICATION_JSON)
  public String basicEntity(@Valid SomeEntity entity) {
    return "oh oh";
  }

  // Triggers the "else-ish" part in ResteasyExceptionMapper.toResponse()
  @Path("constraintDefinitionException")
  @GET
  @Consumes(MediaType.APPLICATION_JSON)
  public String constraintDefinitionException() {
    throw new ConstraintDefinitionException("meep");
  }

  // Triggers the "else-ish" part in ResteasyExceptionMapper.toResponse()
  @Path("constraintDeclarationException")
  @GET
  @Consumes(MediaType.APPLICATION_JSON)
  public String constraintDeclarationException() {
    throw new ConstraintDeclarationException("meep");
  }

  // Triggers the "else-ish" part in ResteasyExceptionMapper.toResponse()
  @Path("groupDefinitionException")
  @GET
  @Consumes(MediaType.APPLICATION_JSON)
  public String groupDefinitionException() {
    throw new GroupDefinitionException("meep");
  }

  /**
   * Throws an exception depending on the parameter.
   *
   * @return nothing
   * @see TestNessieError#unhandledRuntimeExceptionInStore()
   * @see TestNessieError#backendThrottledExceptionInStore()
   */
  @Path("unhandledExceptionInTvsStore/{exception}")
  @GET
  @Consumes(MediaType.APPLICATION_JSON)
  public String unhandledExceptionInTvsStore(@PathParam("exception") String exception) {
    Exception ex;
    switch (exception) {
      case "runtime":
        ex = new RuntimeException("Store.getValues-throwing");
        break;
      case "throttle":
        ex = new BackendLimitExceededException("Store.getValues-throttled");
        break;
      default:
        throw new IllegalArgumentException("test code error");
    }

    DatabaseAdapter databaseAdapter = Mockito.mock(DatabaseAdapter.class);
    Mockito.when(databaseAdapter.namedRefs()).thenThrow(ex);

    PersistVersionStore<String, String, StringStoreWorker.TestEnum> tvs =
        new PersistVersionStore<>(databaseAdapter, StringStoreWorker.INSTANCE);
    tvs.getNamedRefs().forEach(ref -> {});
    return "we should not get here";
  }
}
