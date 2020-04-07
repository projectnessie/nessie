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

package com.dremio.iceberg.client;

import com.dremio.iceberg.client.rest.ConflictException;
import com.dremio.iceberg.client.rest.PreconditionFailedException;
import com.dremio.iceberg.json.ObjectMapperContextResolver;
import com.dremio.iceberg.model.Base;
import com.dremio.iceberg.model.VersionedWrapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.Supplier;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.NotAuthorizedException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.EntityTag;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

/**
 * class to collect all common operations for client.
 */
abstract class BaseClient<T extends Base> {

  private final String endpoint;
  private final String name;
  private final String base;
  private final Supplier<String> authHeader;
  private final Class<T[]> arrayClazz;
  private final GenericType<VersionedWrapper<T>> wrapperClazz;
  private final Map<String, OptionalLong> cache = new HashMap<>();
  private final ClientWithHelpers client;

  public BaseClient(ClientWithHelpers client,
                    String name,
                    String base,
                    String endpoint,
                    Supplier<String> authHeader,
                    Class<T[]> arrayClazz,
                    GenericType<VersionedWrapper<T>> wrapperClazz) {
    this.name = name;
    this.base = base;
    this.endpoint = endpoint;
    this.authHeader = authHeader;
    this.arrayClazz = arrayClazz;
    this.wrapperClazz = wrapperClazz;
    this.client = client;
  }

  static void checkResponse(Response response) {
    if (response.getStatus() == Status.OK.getStatusCode()
        || response.getStatus() == Status.CREATED.getStatusCode()) {
      return;
    }
    if (response.getStatus() == Status.NOT_FOUND.getStatusCode()) {
      throw new NotFoundException(response);
    }
    if (response.getStatus() == Status.FORBIDDEN.getStatusCode()) {
      throw new ForbiddenException(response);
    }
    if (response.getStatus() == Status.UNAUTHORIZED.getStatusCode()) {
      throw new NotAuthorizedException(response);
    }
    if (response.getStatus() == Status.BAD_REQUEST.getStatusCode()) {
      throw new BadRequestException(response);
    }
    if (response.getStatus() == Status.PRECONDITION_FAILED.getStatusCode()) {
      throw new PreconditionFailedException(response);
    }
    if (response.getStatus() == Status.CONFLICT.getStatusCode()) {
      throw new ConflictException(response);
    }
    throw new RuntimeException(
      "Unknown exception " + response.getStatus() + " " + response.readEntity(String.class));
  }

  public T[] getAll() {
    return getAll(null);
  }

  public T[] getAll(String namespace) {
    Response response;
    if (namespace != null) {
      response = client.get(endpoint, name + "/", MediaType.APPLICATION_JSON, authHeader.get(),
                            ImmutableMap.of("namespace", namespace)).get();
    } else {
      response =
        client.get(endpoint, name + "/", MediaType.APPLICATION_JSON, authHeader.get()).get();
    }
    checkResponse(response);
    return response.readEntity(arrayClazz);
  }

  public T getObject(String tableName) {
    Optional<VersionedWrapper<T>> table = checkTable(tableName);
    return table.map(VersionedWrapper::getObj).orElse(null);
  }

  private Optional<VersionedWrapper<T>> checkTable(String tableName) {
    Response response = client.get(endpoint,
                                   name + "/" + tableName,
                                   MediaType.APPLICATION_JSON,
                                   authHeader.get())
                              .get();
    return checkTable(response);
  }

  private Optional<VersionedWrapper<T>> checkTable(Response response) {
    try {
      checkResponse(response);
    } catch (NotFoundException e) {
      return Optional.empty();
    }
    VersionedWrapper<T> table = response.readEntity(wrapperClazz);
    if (table != null) {
      String tag = response.getHeaders().getFirst(HttpHeaders.ETAG).toString();
      assert Long.parseLong(tag.replaceAll("\"", "")) == table.getVersion().orElse(0L);
      cache.put(table.getObj().getId(), table.getVersion());
      return Optional.of(table);
    }
    return Optional.empty();
  }

  public T getObjectByName(String tableName, String namespace) {
    Map<String, String> queryParam = new HashMap<>();
    queryParam.put("namespace", namespace);
    Response response = client.get(endpoint,
                                   name + "/by-name/" + tableName,
                                   MediaType.APPLICATION_JSON,
                                   authHeader.get(),
                                   queryParam).get();
    Optional<VersionedWrapper<T>> table = checkTable(response);
    return table.map(VersionedWrapper::getObj).orElse(null);
  }

  public void deleteObject(String tableName, boolean purge) {
    Response response = client.get(endpoint,
                                   name + "/" + tableName,
                                   MediaType.APPLICATION_JSON,
                                   authHeader.get(),
                                   ImmutableMap.of("purge", Boolean.toString(purge))
    )
                              .delete();
    checkResponse(response);
  }

  public void updateObject(T table) {
    OptionalLong version = cache.get(table.getId());
    if (version == null || !version.isPresent()) {
      throw new NotFoundException();
    }
    Invocation.Builder request = client.get(endpoint, name + "/" + table.getId(),
                                            MediaType.APPLICATION_JSON, authHeader.get());
    request.header(HttpHeaders.IF_MATCH, new EntityTag(Long.toString(version.getAsLong())));
    Response response = request.put(Entity.entity(table, MediaType.APPLICATION_JSON));
    checkResponse(response);
  }

  public T createObject(T table) {
    Response response = client.get(endpoint,
                                   name + "/",
                                   MediaType.APPLICATION_JSON,
                                   authHeader.get())
                              .post(Entity.entity(table, MediaType.APPLICATION_JSON));
    checkResponse(response);
    String id = response.getLocation().getRawPath().replace(base, "").replace("//", "");
    response = client.get(endpoint, id, MediaType.APPLICATION_JSON, authHeader.get()).get();
    checkResponse(response);
    VersionedWrapper<T> newTable = response.readEntity(wrapperClazz);
    assert newTable.getVersion().orElse(0L) == Long.parseLong(
      ((String) response.getHeaders().getFirst(HttpHeaders.ETAG))
        .replaceAll("\"", ""));
    return newTable.getObj();
  }

  static class ClientWithHelpers {

    private final Client client;

    ClientWithHelpers() {
      this.client = ClientBuilder.newBuilder().register(ObjectMapperContextResolver.class).build();
    }

    public void close() {
      client.close();
    }

    public Invocation.Builder get(String endpoint, String path, String mediaType,
                                  String authHeader) {
      Map<String, String> params = ImmutableMap.of();
      return get(endpoint, path, mediaType, authHeader, params);
    }

    public Invocation.Builder get(String endpoint, String path, String mediaType, String authHeader,
                                  Map<String, String> queryParams) {
      WebTarget webTarget = client.target(endpoint);
      for (Map.Entry<String, String> entry : queryParams.entrySet()) {
        webTarget = webTarget.queryParam(entry.getKey(), entry.getValue());
      }
      Invocation.Builder builder = webTarget.path(path)
                                            .request(mediaType)
                                            .accept(MediaType.APPLICATION_JSON_TYPE);
      if (authHeader != null) {
        builder.header(HttpHeaders.AUTHORIZATION, authHeader);
      }
      return builder;
    }
  }
}
