/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.quarkus.providers.storage;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.projectnessie.quarkus.providers.storage.CacheInvalidationReceiver.NESSIE_CACHE_INVALIDATION_TOKEN_HEADER;
import static org.projectnessie.quarkus.providers.storage.CacheInvalidations.CacheInvalidationPutObj.cacheInvalidationPutObj;
import static org.projectnessie.quarkus.providers.storage.CacheInvalidations.CacheInvalidationPutReference.cacheInvalidationPutReference;
import static org.projectnessie.quarkus.providers.storage.CacheInvalidations.CacheInvalidationRemoveObj.cacheInvalidationRemoveObj;
import static org.projectnessie.quarkus.providers.storage.CacheInvalidations.CacheInvalidationRemoveReference.cacheInvalidationRemoveReference;
import static org.projectnessie.quarkus.providers.storage.CacheInvalidations.cacheInvalidations;
import static org.projectnessie.versioned.storage.common.persist.ObjId.EMPTY_OBJ_ID;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.Future;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RequestBody;
import io.vertx.ext.web.RoutingContext;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import org.projectnessie.quarkus.config.QuarkusStoreConfig;
import org.projectnessie.versioned.storage.cache.DistributedCacheInvalidation;

public class TestCacheInvalidationReceiver {

  @Test
  public void senderReceiver() throws Exception {
    CacheInvalidationReceiver.DistributedCacheInvalidationHolder invalidationHolder =
        new CacheInvalidationReceiver.DistributedCacheInvalidationHolder();
    DistributedCacheInvalidation distributedCacheInvalidation =
        mock(DistributedCacheInvalidation.class);
    invalidationHolder.accept(distributedCacheInvalidation);

    String token = "cafe";
    List<String> tokens = singletonList(token);
    String receiverId = "receiverId";
    String senderId = "senderId";

    CacheInvalidationReceiver receiver = buildReceiver(tokens, invalidationHolder, receiverId);

    CacheInvalidations invalidations = cacheInvalidations(allInvalidationTypes());

    RoutingContext rc =
        expectResponse(
            r -> {
              when(r.getParam("sender")).thenReturn(senderId);
              when(r.getHeader(NESSIE_CACHE_INVALIDATION_TOKEN_HEADER)).thenReturn(token);
            });
    RequestBody reqBody = mock(RequestBody.class);
    when(reqBody.asString()).thenReturn(new ObjectMapper().writeValueAsString(invalidations));
    when(rc.body()).thenReturn(reqBody);

    receiver.cacheInvalidations(rc);

    verify(rc.response()).setStatusCode(204);
    verify(rc.response()).setStatusMessage("No content");

    verify(distributedCacheInvalidation).putObj("repo", EMPTY_OBJ_ID, 42);
    verify(distributedCacheInvalidation).removeObj("repo", EMPTY_OBJ_ID);
    verify(distributedCacheInvalidation).putReference("repo", "refs/foo/bar", 42);
    verify(distributedCacheInvalidation).removeReference("repo", "refs/foo/bar");
    verifyNoMoreInteractions(distributedCacheInvalidation);
  }

  @Test
  public void doesNotAcceptInvalidationsWithoutTokens() {
    CacheInvalidationReceiver.DistributedCacheInvalidationHolder invalidationHolder =
        new CacheInvalidationReceiver.DistributedCacheInvalidationHolder();
    DistributedCacheInvalidation distributedCacheInvalidation =
        mock(DistributedCacheInvalidation.class);
    invalidationHolder.accept(distributedCacheInvalidation);

    String token = "cafe";
    List<String> tokens = emptyList();
    String receiverId = "receiverId";
    String senderId = "senderId";

    CacheInvalidationReceiver receiver = buildReceiver(tokens, invalidationHolder, receiverId);

    RoutingContext rc = expectResponse();
    receiver.cacheInvalidations(
        rc, () -> cacheInvalidations(allInvalidationTypes()), senderId, token);

    verify(rc.response()).setStatusCode(400);
    verify(rc.response()).setStatusMessage("Invalid token");

    verifyNoMoreInteractions(distributedCacheInvalidation);
  }

  @Test
  public void receiveFromSelf() {
    CacheInvalidationReceiver.DistributedCacheInvalidationHolder invalidationHolder =
        new CacheInvalidationReceiver.DistributedCacheInvalidationHolder();
    DistributedCacheInvalidation distributedCacheInvalidation =
        mock(DistributedCacheInvalidation.class);
    invalidationHolder.accept(distributedCacheInvalidation);

    String token = "cafe";
    List<String> tokens = singletonList(token);
    String receiverId = "receiverId";

    CacheInvalidationReceiver receiver = buildReceiver(tokens, invalidationHolder, receiverId);

    RoutingContext rc = expectResponse();
    receiver.cacheInvalidations(
        rc, () -> cacheInvalidations(allInvalidationTypes()), receiverId, token);

    verify(rc.response()).setStatusCode(204);
    verify(rc.response()).setStatusMessage("No content");

    verifyNoMoreInteractions(distributedCacheInvalidation);
  }

  @Test
  public void unknownToken() {
    CacheInvalidationReceiver.DistributedCacheInvalidationHolder invalidationHolder =
        new CacheInvalidationReceiver.DistributedCacheInvalidationHolder();
    DistributedCacheInvalidation distributedCacheInvalidation =
        mock(DistributedCacheInvalidation.class);
    invalidationHolder.accept(distributedCacheInvalidation);

    String token = "cafe";
    List<String> tokens = singletonList(token);
    String differentToken = "otherToken";
    String receiverId = "receiverId";
    String senderId = "senderId";

    CacheInvalidationReceiver receiver = buildReceiver(tokens, invalidationHolder, receiverId);

    RoutingContext rc = expectResponse();
    receiver.cacheInvalidations(
        rc, () -> cacheInvalidations(allInvalidationTypes()), senderId, differentToken);

    verify(rc.response()).setStatusCode(400);
    verify(rc.response()).setStatusMessage("Invalid token");

    verifyNoMoreInteractions(distributedCacheInvalidation);
  }

  private RoutingContext expectResponse() {
    return expectResponse(r -> {});
  }

  private RoutingContext expectResponse(Consumer<HttpServerRequest> requestMocker) {
    HttpServerResponse response = mock(HttpServerResponse.class);
    when(response.setStatusCode(anyInt())).thenReturn(response);
    when(response.setStatusMessage(anyString())).thenReturn(response);
    when(response.end()).thenReturn(Future.succeededFuture());

    HttpServerRequest request = mock(HttpServerRequest.class);
    when(request.getHeader("Content-Type")).thenReturn("application/json");
    requestMocker.accept(request);

    RoutingContext rc = mock(RoutingContext.class);
    when(rc.response()).thenReturn(response);
    when(rc.request()).thenReturn(request);
    return rc;
  }

  private static CacheInvalidationReceiver buildReceiver(
      List<String> tokens,
      CacheInvalidationReceiver.DistributedCacheInvalidationHolder invalidationHolder,
      String receiverId) {
    QuarkusStoreConfig config = mock(QuarkusStoreConfig.class);
    when(config.cacheInvalidationValidTokens()).thenReturn(Optional.of(tokens));

    return new CacheInvalidationReceiver(config, invalidationHolder, receiverId);
  }

  List<CacheInvalidations.CacheInvalidation> allInvalidationTypes() {
    return List.of(
        cacheInvalidationPutReference("repo", "refs/foo/bar", 42),
        cacheInvalidationRemoveReference("repo", "refs/foo/bar"),
        cacheInvalidationPutObj("repo", EMPTY_OBJ_ID.asByteArray(), 42),
        cacheInvalidationRemoveObj("repo", EMPTY_OBJ_ID.asByteArray()));
  }
}