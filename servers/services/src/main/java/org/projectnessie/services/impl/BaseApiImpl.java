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
package org.projectnessie.services.impl;

import static org.projectnessie.services.cel.CELUtil.CONTAINER;
import static org.projectnessie.services.cel.CELUtil.CONTENT_KEY_DECLARATIONS;
import static org.projectnessie.services.cel.CELUtil.CONTENT_KEY_TYPES;
import static org.projectnessie.services.cel.CELUtil.SCRIPT_HOST;
import static org.projectnessie.services.cel.CELUtil.VAR_KEY;
import static org.projectnessie.services.cel.CELUtil.forCel;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import jakarta.annotation.Nullable;
import java.security.Principal;
import java.time.Instant;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import org.projectnessie.cel.tools.Script;
import org.projectnessie.cel.tools.ScriptException;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.ContentKey;
import org.projectnessie.services.authz.AccessContext;
import org.projectnessie.services.authz.ApiContext;
import org.projectnessie.services.authz.Authorizer;
import org.projectnessie.services.authz.BatchAccessChecker;
import org.projectnessie.services.config.ServerConfig;
import org.projectnessie.services.hash.HashResolver;
import org.projectnessie.versioned.DefaultMetadataRewriter;
import org.projectnessie.versioned.MetadataRewriter;
import org.projectnessie.versioned.VersionStore;

public abstract class BaseApiImpl {
  private final ServerConfig config;
  private final VersionStore store;
  private final Authorizer authorizer;
  private final AccessContext accessContext;
  private final ApiContext apiContext;
  private HashResolver hashResolver;

  protected BaseApiImpl(
      ServerConfig config,
      VersionStore store,
      Authorizer authorizer,
      AccessContext accessContext,
      ApiContext apiContext) {
    this.config = config;
    this.store = store;
    this.authorizer = authorizer;
    this.accessContext = accessContext;
    this.apiContext = apiContext;
  }

  /**
   * Produces the filter predicate for content-key filtering.
   *
   * @param filter The content-key filter expression, if not empty
   */
  static Predicate<ContentKey> filterOnContentKey(String filter) {
    if (Strings.isNullOrEmpty(filter)) {
      return x -> true;
    }

    final Script script;
    try {
      script =
          SCRIPT_HOST
              .buildScript(filter)
              .withContainer(CONTAINER)
              .withDeclarations(CONTENT_KEY_DECLARATIONS)
              .withTypes(CONTENT_KEY_TYPES)
              .build();
    } catch (ScriptException e) {
      throw new IllegalArgumentException(e);
    }
    return key -> {
      try {
        return script.execute(Boolean.class, ImmutableMap.of(VAR_KEY, forCel(key)));
      } catch (ScriptException e) {
        throw new RuntimeException(e);
      }
    };
  }

  protected ServerConfig getServerConfig() {
    return config;
  }

  protected VersionStore getStore() {
    return store;
  }

  protected Principal getPrincipal() {
    return accessContext.user();
  }

  protected Authorizer getAuthorizer() {
    return authorizer;
  }

  protected ApiContext getApiContext() {
    return apiContext;
  }

  protected HashResolver getHashResolver() {
    if (hashResolver == null) {
      this.hashResolver = new HashResolver(config, store);
    }
    return hashResolver;
  }

  protected BatchAccessChecker startAccessCheck() {
    return getAuthorizer().startAccessCheck(accessContext, apiContext);
  }

  protected MetadataRewriter<CommitMeta> commitMetaUpdate(
      @Nullable CommitMeta commitMeta, IntFunction<String> squashMessage) {
    Principal principal = getPrincipal();
    String committer = principal == null ? "" : principal.getName();
    return new DefaultMetadataRewriter(committer, Instant.now(), commitMeta, squashMessage);
  }
}
