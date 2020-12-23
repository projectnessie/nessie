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

import javax.validation.constraints.NotNull;

import com.dremio.nessie.api.ContentsApi;
import com.dremio.nessie.client.http.HttpClient;
import com.dremio.nessie.error.NessieConflictException;
import com.dremio.nessie.error.NessieNotFoundException;
import com.dremio.nessie.model.Contents;
import com.dremio.nessie.model.ContentsKey;
import com.dremio.nessie.model.MultiGetContentsRequest;
import com.dremio.nessie.model.MultiGetContentsResponse;

class ClientContentsApi implements ContentsApi {

  private final HttpClient client;

  public ClientContentsApi(HttpClient client) {
    this.client = client;
  }

  @Override
  public Contents getContents(@NotNull ContentsKey key, String ref) throws NessieNotFoundException {
    return client.newRequest().path("contents").path(key.toPathString())
                 .queryParam("ref", ref)
                 .get()
                 .readEntity(Contents.class);
  }

  @Override
  public MultiGetContentsResponse getMultipleContents(@NotNull String ref, @NotNull MultiGetContentsRequest request)
      throws NessieNotFoundException {
    return client.newRequest().path("contents")
                 .queryParam("ref", ref)
                 .post(request)
                 .readEntity(MultiGetContentsResponse.class);
  }


  @Override
  public void setContents(@NotNull ContentsKey key, String branch, @NotNull String hash, String message,
                          @NotNull Contents contents) throws NessieNotFoundException, NessieConflictException {
    client.newRequest().path("contents").path(key.toPathString())
          .queryParam("branch", branch)
          .queryParam("hash", hash)
          .queryParam("message", message)
          .post(contents);
  }

  @Override
  public void deleteContents(ContentsKey key, String branch, String hash, String message)
      throws NessieNotFoundException, NessieConflictException {
    client.newRequest().path("contents").path(key.toPathString())
          .queryParam("branch", branch)
          .queryParam("hash", hash)
          .queryParam("message", message)
          .delete();
  }
}
