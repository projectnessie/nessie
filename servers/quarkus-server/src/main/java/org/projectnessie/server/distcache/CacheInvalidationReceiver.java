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
package org.projectnessie.server.distcache;

import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;
import static java.util.Collections.emptyList;
import static org.projectnessie.versioned.storage.common.persist.ObjId.objIdFromByteArray;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.vertx.http.ManagementInterface;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.RoutingContext;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import org.projectnessie.quarkus.config.QuarkusStoreConfig;
import org.projectnessie.quarkus.providers.ServerInstanceId;
import org.projectnessie.server.distcache.CacheInvalidations.CacheInvalidationEvictReference;
import org.projectnessie.versioned.storage.cache.DistributedCacheInvalidation;
import org.projectnessie.versioned.storage.cache.DistributedCacheInvalidationConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// See https://quarkus.io/guides/management-interface-reference#management-endpoint-application
@Singleton
public class CacheInvalidationReceiver implements DistributedCacheInvalidationConsumer {
  public static final String NESSIE_CACHE_INVALIDATION_TOKEN_HEADER =
      "Nessie-Cache-Invalidation-Token";

  private static final Logger LOGGER = LoggerFactory.getLogger(CacheInvalidationReceiver.class);

  private DistributedCacheInvalidation distributedCacheInvalidation;
  private final String serverInstanceId;
  private final Set<String> validTokens;
  private final String invalidationPath;
  private final ObjectMapper objectMapper;

  @Inject
  public CacheInvalidationReceiver(
      QuarkusStoreConfig storeConfig, @ServerInstanceId String serverInstanceId) {
    this.serverInstanceId = serverInstanceId;
    this.invalidationPath = storeConfig.cacheInvalidationUri();
    this.validTokens =
        new HashSet<>(storeConfig.cacheInvalidationValidTokens().orElse(emptyList()));
    this.objectMapper =
        new ObjectMapper()
            // forward compatibility
            .disable(FAIL_ON_UNKNOWN_PROPERTIES);
  }

  @Override
  public void applyDistributedCacheInvalidation(
      DistributedCacheInvalidation distributedCacheInvalidation) {
    this.distributedCacheInvalidation = distributedCacheInvalidation;
  }

  public void registerManagementRoutes(@Observes ManagementInterface mi) {
    mi.router().post(invalidationPath).handler(this::cacheInvalidations);
  }

  void cacheInvalidations(RoutingContext rc) {
    HttpServerRequest request = rc.request();
    String senderId = request.getParam("sender");
    String token = request.getHeader(NESSIE_CACHE_INVALIDATION_TOKEN_HEADER);

    cacheInvalidations(
        rc,
        () -> {
          try {
            String json = rc.body().asString();
            if (json == null || json.isEmpty()) {
              return CacheInvalidations.cacheInvalidations(emptyList());
            }
            return objectMapper.readValue(json, CacheInvalidations.class);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        },
        senderId,
        token);
  }

  void cacheInvalidations(
      RoutingContext rc,
      Supplier<CacheInvalidations> invalidations,
      String senderId,
      String token) {
    if (token == null || !validTokens.contains(token)) {
      LOGGER.warn("Received cache invalidation with invalid token {}", token);
      responseInvalidToken(rc);
      return;
    }
    if (serverInstanceId.equals(senderId)) {
      LOGGER.trace("Ignoring invalidations from local instance");
      responseNoContent(rc);
      return;
    }
    if (!"application/json".equals(rc.request().getHeader("Content-Type"))) {
      LOGGER.warn("Received cache invalidation with invalid HTTP content type");
      responseInvalidContentType(rc);
      return;
    }

    List<CacheInvalidations.CacheInvalidation> invs;
    try {
      invs = invalidations.get().invalidations();
    } catch (RuntimeException e) {
      responseServerError(rc);
      return;
    }

    DistributedCacheInvalidation cacheInvalidation = distributedCacheInvalidation;
    if (cacheInvalidation != null) {
      for (CacheInvalidations.CacheInvalidation invalidation : invs) {
        switch (invalidation.type()) {
          case CacheInvalidations.CacheInvalidationEvictObj.TYPE:
            CacheInvalidations.CacheInvalidationEvictObj putObj =
                (CacheInvalidations.CacheInvalidationEvictObj) invalidation;
            cacheInvalidation.evictObj(putObj.repoId(), objIdFromByteArray(putObj.id()));
            break;
          case CacheInvalidations.CacheInvalidationEvictReference.TYPE:
            CacheInvalidationEvictReference putReference =
                (CacheInvalidations.CacheInvalidationEvictReference) invalidation;
            cacheInvalidation.evictReference(putReference.repoId(), putReference.refName());
            break;
          default:
            // nothing we can do about a new invalidation type here
            break;
        }
      }
    }

    responseNoContent(rc);
  }

  private void responseServerError(RoutingContext rc) {
    rc.response().setStatusCode(500).setStatusMessage("Server error parsing request body").end();
  }

  private void responseInvalidToken(RoutingContext rc) {
    rc.response().setStatusCode(400).setStatusMessage("Invalid token").end();
  }

  private void responseInvalidContentType(RoutingContext rc) {
    rc.response().setStatusCode(415).setStatusMessage("Unsupported media type").end();
  }

  private void responseNoContent(RoutingContext rc) {
    rc.response().setStatusCode(204).setStatusMessage("No content").end();
  }
}
