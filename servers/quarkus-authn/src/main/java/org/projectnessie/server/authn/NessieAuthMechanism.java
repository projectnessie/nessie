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
package org.projectnessie.server.authn;

import io.quarkus.security.AuthenticationFailedException;
import io.quarkus.security.identity.AuthenticationRequestContext;
import io.quarkus.security.identity.IdentityProvider;
import io.quarkus.security.identity.IdentityProviderManager;
import io.quarkus.security.identity.SecurityIdentity;
import io.quarkus.security.identity.request.AnonymousAuthenticationRequest;
import io.quarkus.security.identity.request.AuthenticationRequest;
import io.quarkus.security.runtime.AnonymousIdentityProvider;
import io.quarkus.security.spi.runtime.BlockingSecurityExecutor;
import io.quarkus.vertx.http.runtime.security.ChallengeData;
import io.quarkus.vertx.http.runtime.security.HttpAuthenticationMechanism;
import io.quarkus.vertx.http.runtime.security.HttpCredentialTransport;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.groups.UniCreate;
import io.vertx.ext.web.RoutingContext;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import org.projectnessie.quarkus.config.QuarkusNessieAuthenticationConfig;

/**
 * HTTP authentication mechanism implementation to disallow all unauthorized accesses, except for
 * the explicitly configured paths and static resources.
 */
@ApplicationScoped
public class NessieAuthMechanism implements HttpAuthenticationMechanism {

  /**
   * {@code HttpAuthenticationMechanism}s with a higher priority are "asked" first, let
   * `NessieAuthMechanism` be called quite late, basically as the last one.
   */
  public static final int PRIORITY = 0;

  private final boolean enabled;
  private final AuthenticationFailedException unauthorized;
  private final Set<String> anonymousPaths;
  private final Set<String> anonymousPathPrefixes;

  /** Provides the anonymous {@link SecurityIdentity}. */
  private final AnonymousIdentityProvider anonymousIdentityProvider;

  /**
   * The blocking authentication request context, which is technically not needed by the
   * implementation of {@link AnonymousIdentityProvider}, but we provide it to be on the safe side.
   */
  private final AuthenticationRequestContext blockingRequestContext;

  @Inject
  public NessieAuthMechanism(
      QuarkusNessieAuthenticationConfig config, BlockingSecurityExecutor blockingExecutor) {
    this.enabled = config.enabled();
    this.unauthorized = new AuthenticationFailedException("Missing or unrecognized credentials");
    Set<String> paths = new HashSet<>();
    config.anonymousPaths().ifPresent(paths::addAll);
    config.anonymousPathPrefixes().ifPresent(paths::addAll);
    this.anonymousPaths = paths;
    this.anonymousPathPrefixes =
        config
            .anonymousPathPrefixes()
            .map(prefixes -> prefixes.stream().map(p -> p + '/').collect(Collectors.toSet()))
            .orElse(Set.of());
    this.anonymousIdentityProvider = new AnonymousIdentityProvider();
    this.blockingRequestContext = blockingExecutor::executeBlocking;
  }

  boolean checkPath(String path) {
    // Allow certain preconfigured paths (e.g. health checks) to be serviced without authentication.
    return anonymousPaths.contains(path)
        || anonymousPathPrefixes.stream().anyMatch(path::startsWith);
  }

  @Override
  public int getPriority() {
    return PRIORITY;
  }

  /**
   * Nessie specific way to conditionally allow anonymous access.
   *
   * <p>First, anonymous access is always allowed for all resources, if {@link
   * QuarkusNessieAuthenticationConfig#enabled()} is {@code false}.
   *
   * <p>If authentication is {@linkplain QuarkusNessieAuthenticationConfig#enabled() enabled}, then
   * access to static resources and configured {@linkplain
   * QuarkusNessieAuthenticationConfig#anonymousPaths() paths (exact match)} and {@linkplain
   * QuarkusNessieAuthenticationConfig#anonymousPathPrefixes() path prefixes} does not need
   * authenticated requests. All other requests must be authenticated, handled by an {@linkplain
   * HttpAuthenticationMechanism auth mechanism} with a <em>higher</em> {@linkplain #getPriority()
   * priority}, or this function lets the request {@linkplain AuthenticationFailedException fail}.
   *
   * <p>Authentication mechanisms are called by Quarkus and return a {@link Uni} that can either
   * {@linkplain UniCreate#failure(Throwable) fail immediately}, {@linkplain UniCreate#item(Object)
   * return} an authenticated security identity or return {@linkplain UniCreate#nullItem() null},
   * which means that the {@link IdentityProvider}s with {@linkplain #getCredentialTypes() matching}
   * {@linkplain AuthenticationRequest credential types} shall be called to actually generate the
   * {@link SecurityIdentity}.
   */
  @Override
  public Uni<SecurityIdentity> authenticate(
      RoutingContext context, IdentityProviderManager identityProviderManager) {
    if (enabled) {
      // Some (REST) resources shall be publicly accessible. In hindsight, this should have been
      // configured using corresponding annotations on the endpoint and/or Quarkus path-based
      // auth-mechanism configuration (see
      // https://quarkus.io/guides/security-authentication-mechanisms#use-http-security-policy-to-enable-path-based-authentication).
      // To not cause any configuration changes, we still stick to our previous way to configure
      // this.
      var path = context.request().path();
      if (checkPath(path)) {
        return anonymous();
      }

      // If none of the above matches, let the whole request fail. This prevents auth-mechanisms
      // with a _lower_ priority from being asked (OIDC and others have a higher priority).
      return Uni.createFrom().failure(unauthorized);
    }

    return anonymous();
  }

  /**
   * Effectively return the {@linkplain AnonymousIdentityProvider anonymous SecurityIdentity}
   * directly, which is, as of Quarkus 3.16, a constant value.
   */
  private Uni<SecurityIdentity> anonymous() {
    return anonymousIdentityProvider.authenticate(
        AnonymousAuthenticationRequest.INSTANCE, blockingRequestContext);
  }

  @Override
  public Uni<ChallengeData> getChallenge(RoutingContext context) {
    return Uni.createFrom().nothing();
  }

  @Override
  public Uni<HttpCredentialTransport> getCredentialTransport(RoutingContext context) {
    return Uni.createFrom().nothing();
  }
}
