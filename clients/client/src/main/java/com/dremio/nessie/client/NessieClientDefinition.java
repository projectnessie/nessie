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

import java.util.List;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.dremio.nessie.model.Branch;
import com.dremio.nessie.model.NessieConfiguration;
import com.dremio.nessie.model.Reference;
import com.dremio.nessie.model.ReferenceWithType;
import com.dremio.nessie.model.Table;

public interface NessieClientDefinition {

  /**
   * get all refs.
   */
  @GET
  @Consumes(MediaType.APPLICATION_JSON)
  @Path("objects")
  List<ReferenceWithType<Reference>> refs();

  /**
   * get ref details.
   */
  @GET
  @Consumes(MediaType.APPLICATION_JSON)
  @Path("objects/{ref}")
  ReferenceWithType<Branch> ref(@PathParam("ref") String refName);

  /**
   * get a table in a specific ref.
   */
  @GET
  @Consumes(MediaType.APPLICATION_JSON)
  @Path("objects/{ref}/tables/{table}")
  Table refTable(@PathParam("ref") String ref,
                    @PathParam("table") String table,
                    @QueryParam("metadata") boolean metadata
                    );

  /**
   * create a ref.
   */
  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Path("objects/{ref}")
  void createRef(@PathParam("ref") String refName,
                     @HeaderParam("If-Match") String hash,
                     ReferenceWithType ref);

  /**
   * create a table on a specific ref.
   */
  @POST
  @Path("objects/{ref}/tables/{table}")
  @Consumes(MediaType.APPLICATION_JSON)
  Response createTable(@PathParam("ref") String ref,
                       @PathParam("table") String tableName,
                       @QueryParam("reason") String reason,
                       @HeaderParam("If-Match") String hash,
                       Table table);


  /**
   * delete a ref.
   */
  @DELETE
  @Path("objects/{ref}")
  @Consumes(MediaType.APPLICATION_JSON)
  void deleteRef(@PathParam("ref") String ref, @HeaderParam("If-Match") String hash);

  /**
   * delete a single table.
   */
  @DELETE
  @Path("objects/{ref}/tables/{table}")
  @Consumes(MediaType.APPLICATION_JSON)
  Response deleteTable(@PathParam("ref") String ref,
                       @PathParam("table") String table,
                       @QueryParam("reason") String reason,
                       @HeaderParam("If-Match") String hash
                       );

  /**
   * assign a Ref to a hash.
   */
  @PUT
  @Path("objects/{ref}")
  @Consumes(MediaType.APPLICATION_JSON)
  void updateBatch(@PathParam("ref") String ref,
                       @QueryParam("target") String targetRef,
                       @HeaderParam("If-Match") String hash
                       );

  /**
   * update a single table on a ref.
   */
  @PUT
  @Path("objects/{ref}/tables/{table}")
  @Consumes(MediaType.APPLICATION_JSON)
  Response update(@PathParam("ref") String ref,
                  @PathParam("table") String table,
                  @QueryParam("reason") String reason,
                  @HeaderParam("If-Match") String hash,
                  Table update);

  /**
   * commit log for a ref.
   */
  @GET
  @Consumes(MediaType.APPLICATION_JSON)
  @Path("objects/{ref}/log")
  Response commitLog(@PathParam("ref") String ref);

  /**
   * cherry pick mergeRef  onto ref.
   */
  @PUT
  @Path("objects/{ref}/transplant")
  @Consumes(MediaType.APPLICATION_JSON)
  Response transplantRef(@PathParam("ref") String ref,
                         @QueryParam("promote") String mergeHashes,
                         @HeaderParam("If-Match") String hash
                         );

  /**
   * merge mergeRef onto ref, optionally forced.
   */
  @PUT
  @Path("objects/{ref}/merge")
  @Consumes(MediaType.APPLICATION_JSON)
  Response mergeRef(@PathParam("ref") String ref,
                    @QueryParam("promote") String mergeHash,
                    @HeaderParam("If-Match") String hash
                    );

  /**
   * get all tables on a ref.
   */
  @GET
  @Consumes(MediaType.APPLICATION_JSON)
  @Path("objects/{ref}/tables")
  List<String> refTables(@PathParam("ref") String refName,
                         @QueryParam("namespace") String namespace);

  /**
   * update multiple tables on a ref.
   */
  @PUT
  @Path("objects/{ref}/tables")
  @Consumes(MediaType.APPLICATION_JSON)
  void updateMulti(@PathParam("ref") String ref,
                       @QueryParam("reason") String reason,
                       @HeaderParam("If-Match") String hash,
                       Table[] update);

  @GET
  @Path("config")
  @Consumes(MediaType.APPLICATION_JSON)
  NessieConfiguration getConfig();
  
}
