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

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import javax.ws.rs.ForbiddenException;
import javax.ws.rs.NotAuthorizedException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.EntityTag;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.dremio.iceberg.model.Base;
import com.dremio.iceberg.model.BaseCollection;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

public abstract class BaseClient<T extends Base> {

  private final String endpoint;
  private String name;
  private final String base;
  private final Supplier<String> authHeader;
  private final Class<T> clazz;
  private final Class<? extends BaseCollection<T>> collectionClazz;

  public BaseClient(String name,
                    String base,
                    String endpoint,
                    Supplier<String> authHeader,
                    Class<T> clazz,
                    Class<? extends BaseCollection<T>> collectionClazz) {
    this.name = name;
    this.base = base;
    this.endpoint = endpoint;
    this.authHeader = authHeader;
    this.clazz = clazz;
    this.collectionClazz = collectionClazz;
  }

  static void checkResponse(Response response) {
    if (response.getStatus() == 200 || response.getStatus() == 201) {
      return;
    }
    if (response.getStatus() == 404) {
      throw new NotFoundException(response);
    }
    if (response.getStatus() == 403) {
      throw new ForbiddenException(response);
    }
    if (response.getStatus() == 401) {
      throw new NotAuthorizedException(response);
    }
    if (response.getStatus() == 412) {
      throw new ConcurrentModificationException();
    }
    throw new RuntimeException("Unknown exception");
  }

  public List<T> getAll() {
    return getAll(null);
  }

  public List<T> getAll(String namespace) {
    try (BaseClient.AutoCloseableClient client = new BaseClient.AutoCloseableClient()) {
      Response response;
      if (namespace != null) {
        response = client.get(endpoint, name + "/", MediaType.APPLICATION_JSON, authHeader.get(),
          ImmutableMap.of("namespace", namespace)).get();
      } else {
        response = client.get(endpoint, name + "/", MediaType.APPLICATION_JSON, authHeader.get()).get();
      }
      checkResponse(response);
      BaseCollection<T> tables = response.readEntity(collectionClazz);
      return tables.get();
    }
  }

  public T getObject(String tableName) {
    try (AutoCloseableClient client = new AutoCloseableClient()) {
      Response response = client.get(endpoint, name + "/" + tableName, MediaType.APPLICATION_JSON, authHeader.get())
        .get();
      return checkTable(response);
    }
  }

  private T checkTable(Response response) {
    try {
      checkResponse(response);
    } catch (NotFoundException e) {
      return null;
    }
    T table = response.readEntity(clazz);
    if (table != null) {
      String tag = response.getHeaders().getFirst(HttpHeaders.ETAG).toString();
      assert Long.parseLong(tag.replaceAll("\"", "")) == table.getVersion();
    }
    return table;
  }

  public T getObjectByName(String tableName, String namespace) {
    Map<String, String> queryParam = Maps.newHashMap();
    queryParam.put("namespace", namespace);
    try (AutoCloseableClient client = new AutoCloseableClient()) {
      Response response = client.get(endpoint, name + "/by-name/" + tableName, MediaType.APPLICATION_JSON,
        authHeader.get(), queryParam).get();
      return checkTable(response);
    }
  }

  public void deleteObject(String tableName, boolean purge) {
    try (AutoCloseableClient client = new AutoCloseableClient()) {
      Response response = client
        .get(endpoint, name + "/" + tableName, MediaType.APPLICATION_JSON, authHeader.get(),
          ImmutableMap.of("purge", Boolean.toString(purge))
        )
        .delete();
      checkResponse(response);
    }
  }

  public void updateObject(T table) {
    table.setUpdateTime(ZonedDateTime.now(ZoneId.of("UTC")).toInstant().toEpochMilli());
    try (AutoCloseableClient client = new AutoCloseableClient()) {
      Invocation.Builder request = client.get(endpoint, name + "/" + table.getUuid(),
        MediaType.APPLICATION_JSON, authHeader.get());
      if (table.getVersion() != null) {
        request.header(HttpHeaders.IF_MATCH, new EntityTag(table.getVersion().toString()));
      }
      Response response = request.put(Entity.entity(table, MediaType.APPLICATION_JSON));
      checkResponse(response);
    }
  }

  public T createObject(T table) {
    table.setUpdateTime(ZonedDateTime.now(ZoneId.of("UTC")).toInstant().toEpochMilli());
    try (AutoCloseableClient client = new AutoCloseableClient()) {
      Response response = client.get(endpoint, name + "/", MediaType.APPLICATION_JSON, authHeader.get())
        .post(Entity.entity(table, MediaType.APPLICATION_JSON));
      checkResponse(response);
      String id = response.getLocation().getRawPath().replace(base, "").replace("//", "");
      response = client.get(endpoint, id, MediaType.APPLICATION_JSON, authHeader.get()).get();
      checkResponse(response);
      T newTable = response.readEntity(clazz);
      assert newTable.getVersion() == Long.parseLong(((String) response.getHeaders().getFirst(HttpHeaders.ETAG)).replaceAll("\"", ""));
      return newTable;
    }
  }

  static class AutoCloseableClient implements AutoCloseable {
    private final Client client;

    AutoCloseableClient() {
      this.client = ClientBuilder.newClient();
    }

    @Override
    public void close() {
      client.close();
    }

    public Invocation.Builder get(String endpoint, String path, String mediaType, String authHeader) {
      Map<String, String> params = ImmutableMap.of();
      return get(endpoint, path, mediaType, authHeader, params);
    }

    public Invocation.Builder get(String endpoint, String path, String mediaType, String authHeader,
                                  Map<String, String> queryParams) {
      WebTarget webTarget = client.target(endpoint);
      for (Map.Entry<String, String> entry : queryParams.entrySet()) {
        webTarget = webTarget.queryParam(entry.getKey(), entry.getValue());
      }
      Invocation.Builder builder = webTarget.path(path).request(mediaType);
      if (authHeader != null) {
        builder.header(HttpHeaders.AUTHORIZATION, authHeader);
      }
      return builder;
    }
  }
}
