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
package org.projectnessie.server.authn;

import io.quarkus.security.AuthenticationFailedException;
import io.quarkus.security.identity.IdentityProvider;
import io.quarkus.security.identity.IdentityProviderManager;
import io.quarkus.security.identity.SecurityIdentity;
import io.quarkus.security.identity.request.AnonymousAuthenticationRequest;
import io.quarkus.vertx.http.runtime.security.HttpAuthenticationMechanism;
import io.quarkus.vertx.http.runtime.security.HttpAuthenticator;
import io.quarkus.vertx.http.runtime.security.PathMatchingHttpSecurityPolicy;
import io.smallrye.mutiny.Uni;
import io.vertx.ext.web.RoutingContext;
import jakarta.annotation.Priority;
import jakarta.enterprise.inject.Alternative;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.Set;
import java.util.function.Supplier;
import org.projectnessie.server.config.QuarkusNessieAuthenticationConfig;

/**
 * A custom {@link HttpAuthenticator}. This authenticator that performs the following main duties:
 *
 * <ul>
 *   <li>Prevents the Quarkus OIDC authentication mechanism from attempting authentication when it
 *       is not configured. Note that attempts to use the OIDC authentication mechanism when the
 *       authentication server is not properly configured will result in 500 errors as opposed to
 *       401 (not authorized).
 *   <li>Completely disallows unauthenticated requests when authentication is enabled.
 * </ul>
 */
@Alternative // @Alternative + @Priority ensure the original HttpAuthenticator bean is not used
@Priority(1)
@Singleton
public class NessieHttpAuthenticator extends HttpAuthenticator {

  private final BaseNessieHttpAuthenticator base;

  @Inject
  public NessieHttpAuthenticator(
      QuarkusNessieAuthenticationConfig config,
      IdentityProviderManager identityProviderManager,
      Instance<PathMatchingHttpSecurityPolicy> pathMatchingPolicy,
      Instance<HttpAuthenticationMechanism> httpAuthenticationMechanism,
      Instance<IdentityProvider<?>> providers) {
    super(identityProviderManager, pathMatchingPolicy, httpAuthenticationMechanism, providers);
    this.base =
        new BaseNessieHttpAuthenticator(
            config,
            () -> identityProviderManager.authenticate(AnonymousAuthenticationRequest.INSTANCE));
  }

  @Override
  public Uni<SecurityIdentity> attemptAuthentication(RoutingContext context) {
    if (!base.authEnabled) {
      return base.anonymous();
    }

    return super.attemptAuthentication(context)
        .onItem()
        .transformToUni(
            securityIdentity -> base.maybeTransform(securityIdentity, context.request().path()));
  }

  static class BaseNessieHttpAuthenticator {
    private final boolean authEnabled;
    private final Set<String> anonymousPaths;
    private final Supplier<Uni<SecurityIdentity>> anonymousSupplier;

    BaseNessieHttpAuthenticator(
        QuarkusNessieAuthenticationConfig config,
        Supplier<Uni<SecurityIdentity>> anonymousSupplier) {
      this.authEnabled = config.enabled();
      this.anonymousPaths = config.anonymousPaths();
      this.anonymousSupplier = anonymousSupplier;
    }

    Uni<SecurityIdentity> anonymous() {
      return anonymousSupplier.get();
    }

    Uni<SecurityIdentity> maybeTransform(SecurityIdentity securityIdentity, String path) {
      if (securityIdentity == null) {
        // Allow certain preconfigured paths (e.g. health checks) to be serviced without
        // authentication.
        if (path != null && anonymousPaths.contains(path)) {
          return anonymous();
        }

        // Disallow unauthenticated requests when requested by configuration.
        // Note: Quarkus by default permits unauthenticated requests unless there are
        // specific authorization rules that validate the security identity.
        throw new AuthenticationFailedException("Missing or unrecognized credentials");
      }

      return Uni.createFrom().item(securityIdentity);
    }
  }
}
