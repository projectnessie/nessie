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
package org.projectnessie.restcatalog.api;

import java.io.IOException;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.HEAD;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.RenameTableRequest;
import org.apache.iceberg.rest.requests.ReportMetricsRequest;
import org.apache.iceberg.rest.requests.UpdateNamespacePropertiesRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.CreateNamespaceResponse;
import org.apache.iceberg.rest.responses.GetNamespaceResponse;
import org.apache.iceberg.rest.responses.ListNamespacesResponse;
import org.apache.iceberg.rest.responses.ListTablesResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.rest.responses.UpdateNamespacePropertiesResponse;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;

/** Iceberg REST API v1 JAX-RS interface, using request and response types from Iceberg. */
@Consumes({MediaType.APPLICATION_JSON})
@jakarta.ws.rs.Consumes({jakarta.ws.rs.core.MediaType.APPLICATION_JSON})
@Produces({MediaType.APPLICATION_JSON})
@jakarta.ws.rs.Produces({jakarta.ws.rs.core.MediaType.APPLICATION_JSON})
@Path("iceberg/v1")
@jakarta.ws.rs.Path("iceberg/v1")
@Tag(name = "Iceberg v1")
public interface IcebergV1Api {

  @POST
  @jakarta.ws.rs.POST
  @Path("/{prefix}/namespaces")
  @jakarta.ws.rs.Path("/{prefix}/namespaces")
  CreateNamespaceResponse createNamespace(
      @PathParam("prefix") @jakarta.ws.rs.PathParam("prefix") String prefix,
      @Valid @jakarta.validation.Valid CreateNamespaceRequest createNamespaceRequest)
      throws IOException;

  @POST
  @jakarta.ws.rs.POST
  @Path("/{prefix}/namespaces/{namespace}/tables")
  @jakarta.ws.rs.Path("/{prefix}/namespaces/{namespace}/tables")
  LoadTableResponse createTable(
      @PathParam("prefix") @jakarta.ws.rs.PathParam("prefix") String prefix,
      @PathParam("namespace") @jakarta.ws.rs.PathParam("namespace") String namespace,
      @Valid @jakarta.validation.Valid CreateTableRequest createTableRequest)
      throws IOException, IcebergConflictException;

  @DELETE
  @jakarta.ws.rs.DELETE
  @Path("/{prefix}/namespaces/{namespace}")
  @jakarta.ws.rs.Path("/{prefix}/namespaces/{namespace}")
  void dropNamespace(
      @PathParam("prefix") @jakarta.ws.rs.PathParam("prefix") String prefix,
      @PathParam("namespace") @jakarta.ws.rs.PathParam("namespace") String namespace)
      throws IOException;

  @DELETE
  @jakarta.ws.rs.DELETE
  @Path("/{prefix}/namespaces/{namespace}/tables/{table}")
  @jakarta.ws.rs.Path("/{prefix}/namespaces/{namespace}/tables/{table}")
  void dropTable(
      @PathParam("prefix") @jakarta.ws.rs.PathParam("prefix") String prefix,
      @PathParam("namespace") @jakarta.ws.rs.PathParam("namespace") String namespace,
      @PathParam("table") @jakarta.ws.rs.PathParam("table") String table,
      @QueryParam("purgeRequested")
          @jakarta.ws.rs.QueryParam("purgeRequested")
          @DefaultValue("false")
          @jakarta.ws.rs.DefaultValue("false")
          Boolean purgeRequested)
      throws IOException;

  @GET
  @jakarta.ws.rs.GET
  @Path("/{prefix}/namespaces")
  @jakarta.ws.rs.Path("/{prefix}/namespaces")
  ListNamespacesResponse listNamespaces(
      @PathParam("prefix") @jakarta.ws.rs.PathParam("prefix") String prefix,
      @QueryParam("parent") @jakarta.ws.rs.QueryParam("parent") String parent)
      throws IOException;

  @GET
  @jakarta.ws.rs.GET
  @Path("/{prefix}/namespaces/{namespace}/tables")
  @jakarta.ws.rs.Path("/{prefix}/namespaces/{namespace}/tables")
  ListTablesResponse listTables(
      @PathParam("prefix") @jakarta.ws.rs.PathParam("prefix") String prefix,
      @PathParam("namespace") @jakarta.ws.rs.PathParam("namespace") String namespace)
      throws IOException;

  @GET
  @jakarta.ws.rs.GET
  @Path("/{prefix}/namespaces/{namespace}")
  @jakarta.ws.rs.Path("/{prefix}/namespaces/{namespace}")
  GetNamespaceResponse loadNamespaceMetadata(
      @PathParam("prefix") @jakarta.ws.rs.PathParam("prefix") String prefix,
      @PathParam("namespace") @jakarta.ws.rs.PathParam("namespace") String namespace)
      throws IOException;

  @GET
  @jakarta.ws.rs.GET
  @Path("/{prefix}/namespaces/{namespace}/tables/{table}")
  @jakarta.ws.rs.Path("/{prefix}/namespaces/{namespace}/tables/{table}")
  LoadTableResponse loadTable(
      @PathParam("prefix") @jakarta.ws.rs.PathParam("prefix") String prefix,
      @PathParam("namespace") @jakarta.ws.rs.PathParam("namespace") String namespace,
      @PathParam("table") @jakarta.ws.rs.PathParam("table") String table)
      throws IOException;

  @POST
  @jakarta.ws.rs.POST
  @Path("/{prefix}/tables/rename")
  @jakarta.ws.rs.Path("/{prefix}/tables/rename")
  void renameTable(
      @PathParam("prefix") @jakarta.ws.rs.PathParam("prefix") String prefix,
      @Valid @jakarta.validation.Valid @NotNull @jakarta.validation.constraints.NotNull
          RenameTableRequest renameTableRequest)
      throws IOException, IcebergConflictException;

  @POST
  @jakarta.ws.rs.POST
  @Path("/{prefix}/namespaces/{namespace}/tables/{table}/metrics")
  @jakarta.ws.rs.Path("/{prefix}/namespaces/{namespace}/tables/{table}/metrics")
  void reportMetrics(
      @PathParam("prefix") @jakarta.ws.rs.PathParam("prefix") String prefix,
      @PathParam("namespace") @jakarta.ws.rs.PathParam("namespace") String namespace,
      @PathParam("table") @jakarta.ws.rs.PathParam("table") String table,
      @Valid @jakarta.validation.Valid @NotNull @jakarta.validation.constraints.NotNull
          ReportMetricsRequest reportMetricsRequest)
      throws IOException;

  @HEAD
  @jakarta.ws.rs.HEAD
  @Path("/{prefix}/namespaces/{namespace}/tables/{table}")
  @jakarta.ws.rs.Path("/{prefix}/namespaces/{namespace}/tables/{table}")
  void tableExists(
      @PathParam("prefix") @jakarta.ws.rs.PathParam("prefix") String prefix,
      @PathParam("namespace") @jakarta.ws.rs.PathParam("namespace") String namespace,
      @PathParam("table") @jakarta.ws.rs.PathParam("table") String table)
      throws IOException;

  @POST
  @jakarta.ws.rs.POST
  @Path("/{prefix}/namespaces/{namespace}/properties")
  @jakarta.ws.rs.Path("/{prefix}/namespaces/{namespace}/properties")
  UpdateNamespacePropertiesResponse updateProperties(
      @PathParam("prefix") @jakarta.ws.rs.PathParam("prefix") String prefix,
      @PathParam("namespace") @jakarta.ws.rs.PathParam("namespace") String namespace,
      @Valid @jakarta.validation.Valid
          UpdateNamespacePropertiesRequest updateNamespacePropertiesRequest)
      throws IOException;

  @POST
  @jakarta.ws.rs.POST
  @Path("/{prefix}/namespaces/{namespace}/tables/{table}")
  @jakarta.ws.rs.Path("/{prefix}/namespaces/{namespace}/tables/{table}")
  LoadTableResponse updateTable(
      @PathParam("prefix") @jakarta.ws.rs.PathParam("prefix") String prefix,
      @PathParam("namespace") @jakarta.ws.rs.PathParam("namespace") String namespace,
      @PathParam("table") @jakarta.ws.rs.PathParam("table") String table,
      @Valid @jakarta.validation.Valid UpdateTableRequest commitTableRequest)
      throws IOException, IcebergConflictException;
}
