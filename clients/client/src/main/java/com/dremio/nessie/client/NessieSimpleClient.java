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
package com.dremio.nessie.client;

import javax.annotation.security.RolesAllowed;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.dremio.nessie.model.ReferenceWithType;
import com.dremio.nessie.model.Table;

public interface NessieSimpleClient {

  /**
   * get all refs.
   */
  @GET
  @RolesAllowed({"admin", "user"})
  @Consumes(MediaType.APPLICATION_JSON)
  @Path("objects")
  Response refs(@HeaderParam("Authorization") String auth);

  /**
   * get ref details.
   */
  @GET
  @RolesAllowed({"admin", "user"})
  @Consumes(MediaType.APPLICATION_JSON)
  @Path("objects/{ref}")
  Response ref(@PathParam("ref") String refName, @HeaderParam("Authorization") String auth);

  /**
   * get a table in a specific ref.
   */
  @GET
  @RolesAllowed({"admin", "user"})
  @Consumes(MediaType.APPLICATION_JSON)
  @Path("objects/{ref}/tables/{table}")
  Response refTable(@PathParam("ref") String ref,
                    @PathParam("table") String table,
                    @DefaultValue("false") @QueryParam("metadata") boolean metadata,
                    @HeaderParam("Authorization") String auth);

  /**
   * create a ref.
   */
  @POST
  @RolesAllowed({"admin"})
  @Consumes(MediaType.APPLICATION_JSON)
  @Path("objects/{ref}")
  Response createRef(@PathParam("ref") String refName,
                     @HeaderParam("If-Match") String hash,
                     @HeaderParam("Authorization") String auth,
                     ReferenceWithType ref);

  /**
   * create a table on a specific ref.
   */
  @POST
  @RolesAllowed({"admin"})
  @Path("objects/{ref}/tables/{table}")
  @Consumes(MediaType.APPLICATION_JSON)
  Response createTable(@PathParam("ref") String ref,
                       @PathParam("table") String tableName,
                       @DefaultValue("unknown") @QueryParam("reason") String reason,
                       @HeaderParam("If-Match") String hash,
                       @HeaderParam("Authorization") String auth,
                       Table table);


  /**
   * delete a ref.
   */
  @DELETE
  @RolesAllowed({"admin"})
  @Path("objects/{ref}")
  @Consumes(MediaType.APPLICATION_JSON)
  Response deleteRef(@PathParam("ref") String ref, @HeaderParam("If-Match") String hash, @HeaderParam("Authorization") String auth);

  /**
   * delete a single table.
   */
  @DELETE
  @RolesAllowed({"admin"})
  @Path("objects/{ref}/tables/{table}")
  @Consumes(MediaType.APPLICATION_JSON)
  Response deleteTable(@PathParam("ref") String ref,
                       @PathParam("table") String table,
                       @DefaultValue("unknown") @QueryParam("reason") String reason,
                       @HeaderParam("If-Match") String hash,
                       @HeaderParam("Authorization") String auth);

  /**
   * assign a Ref to a hash.
   */
  @PUT
  @RolesAllowed({"admin"})
  @Path("objects/{ref}")
  @Consumes(MediaType.APPLICATION_JSON)
  Response updateBatch(@PathParam("ref") String ref,
                       @QueryParam("target") String targetRef,
                       @HeaderParam("If-Match") String hash,
                       @HeaderParam("Authorization") String auth);

  /**
   * update a single table on a ref.
   */
  @PUT
  @RolesAllowed({"admin"})
  @Path("objects/{ref}/tables/{table}")
  @Consumes(MediaType.APPLICATION_JSON)
  Response update(@PathParam("ref") String ref,
                  @PathParam("table") String table,
                  @DefaultValue("unknown") @QueryParam("reason") String reason,
                  @HeaderParam("If-Match") String hash,
                  @HeaderParam("Authorization") String auth,
                  Table update);

  /**
   * commit log for a ref.
   */
  @GET
  @RolesAllowed({"admin"})
  @Consumes(MediaType.APPLICATION_JSON)
  @Path("objects/{ref}/log")
  Response commitLog(@PathParam("ref") String ref, @HeaderParam("Authorization") String auth);

  /**
   * cherry pick mergeRef  onto ref.
   */
  @PUT
  @RolesAllowed({"admin"})
  @Path("objects/{ref}/transplant")
  @Consumes(MediaType.APPLICATION_JSON)
  Response transplantRef(@PathParam("ref") String ref,
                         @QueryParam("promote") String mergeHashes,
                         @HeaderParam("If-Match") String hash,
                         @HeaderParam("Authorization") String auth);

  /**
   * merge mergeRef onto ref, optionally forced.
   */
  @PUT
  @RolesAllowed({"admin"})
  @Path("objects/{ref}/merge")
  @Consumes(MediaType.APPLICATION_JSON)
  Response mergeRef(@PathParam("ref") String ref,
                    @QueryParam("promote") String mergeHash,
                    @HeaderParam("If-Match") String hash,
                    @HeaderParam("Authorization") String auth);

  /**
   * get all tables on a ref.
   */
  @GET
  @RolesAllowed({"admin", "user"})
  @Consumes(MediaType.APPLICATION_JSON)
  @Path("objects/{ref}/tables")
  Response refTables(@PathParam("ref") String refName,
                     @HeaderParam("Authorization") String auth,
                     @DefaultValue("all") @QueryParam("namespace") String namespace);

  /**
   * update multiple tables on a ref.
   */
  @PUT
  @RolesAllowed({"admin"})
  @Path("objects/{ref}/tables")
  @Consumes(MediaType.APPLICATION_JSON)
  Response updateMulti(@PathParam("ref") String ref,
                       @DefaultValue("unknown") @QueryParam("reason") String reason,
                       @HeaderParam("If-Match") String hash,
                       @HeaderParam("Authorization") String auth,
                       Table[] update);

  @GET
  @RolesAllowed({"admin", "user"})
  @Path("config")
  @Consumes(MediaType.APPLICATION_JSON)
  Response getConfig(@HeaderParam("Authorization") String auth);

  /**
   * POST operation for login. Follows the password type for Oauth2.
   */
  @POST
  @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
  @Produces(MediaType.APPLICATION_JSON)
  @Path("login")
  Response login(@FormParam("username") String login, @FormParam("password") String password, @FormParam("grant_type") String grantType);
}
