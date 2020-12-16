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
package com.dremio.nessie.client.http;

import java.io.IOException;
import java.net.HttpURLConnection;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;

/**
 * Simple holder for http response object.
 */
public class HttpResponse {

  private final HttpURLConnection con;
  private final ObjectMapper mapper;

  HttpResponse(HttpURLConnection con, ObjectMapper mapper) {
    this.con = con;
    this.mapper = mapper;
  }

  private <V> V readEntity(ObjectReader reader) {
    try {
      return reader.readValue(con.getInputStream());
    } catch (IOException e) {
      throw new HttpClientException(e);
    }
  }

  public <V> V readEntity(Class<V> clazz) {
    return readEntity(mapper.readerFor(clazz));
  }

  public <V> V readEntity(TypeReference<V> clazz) {
    return readEntity(mapper.readerFor(clazz));
  }

}
