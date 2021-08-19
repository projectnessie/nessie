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
package org.projectnessie.server.authn;

import io.quarkus.security.AuthenticationFailedException;
import io.quarkus.security.identity.IdentityProvider;
import io.quarkus.security.identity.IdentityProviderManager;
import io.quarkus.security.identity.SecurityIdentity;
import io.quarkus.security.identity.request.AnonymousAuthenticationRequest;
import io.quarkus.vertx.http.runtime.security.HttpAuthenticationMechanism;
import io.quarkus.vertx.http.runtime.security.HttpAuthenticator;
import io.smallrye.mutiny.Uni;
import io.vertx.core.http.HttpHeaders;
import io.vertx.ext.web.RoutingContext;
import javax.annotation.Priority;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Alternative;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/**
 * A custom {@link HttpAuthenticator}. This authenticator that performs the following main duties:
 *
 * <ul>
 *   <li>Prevents the Quarkus OIDC authentication mechanism from attempting authentication when it
 *       is not configured.
 *   <li>Completely disallows unauthenticated requests when authentication is enabled.
 * </ul>
 */
@Alternative // @Alternative + @Priority ensure the original HttpAuthenticator bean is not used
@Priority(1)
@ApplicationScoped
public class NessieHttpAuthenticator extends HttpAuthenticator {

  @Inject IdentityProviderManager identityProvider;

  @ConfigProperty(name = "nessie.auth.enabled")
  boolean authEnabled;

  @Inject
  public NessieHttpAuthenticator(
      Instance<HttpAuthenticationMechanism> instance, Instance<IdentityProvider<?>> providers) {
    super(instance, providers);
  }

  @Override
  public Uni<SecurityIdentity> attemptAuthentication(RoutingContext context) {
    if (!authEnabled) {
      String authHeader = context.request().getHeader(HttpHeaders.AUTHORIZATION.toString());
      if (authHeader != null) {
        // Avoid attempting header-based authentication because the server might not have all the
        // required configuration settings in this case.
        // Note: The OIDC Quarkus extension has to be enabled at build time, but we cannot provide
        // working configuration defaults at build time. Therefore, if OIDC were to attempt
        // authenticating a client request with default config settings it would almost always
        // produce a 500 error. Since authentication is not enabled in this case, we immediately
        // return an anonymous SecurityIdentity here.
        return anonymous();
      }
    }

    return super.attemptAuthentication(context)
        .onItem()
        .transformToUni(
            securityIdentity -> {
              if (securityIdentity == null) {
                if (authEnabled) {
                  // Disallow unauthenticated requests when auth is enabled.
                  // Note: Quarkus by default permits unauthenticated requests unless there are
                  // specific authorization rules that validate the security identity.
                  return Uni.createFrom()
                      .failure(new AuthenticationFailedException("Not authenticated"));
                } else {
                  // Note: we cannot allow null SecurityIdentity to be returned to the caller,
                  // because Quarkus will attempt an AnonymousAuthenticationRequest in that
                  // case anyway, but it will try to get the IdentityProviderManager from
                  // this bean. Unfortunately, the getIdentityProviderManager() is not public
                  // in the superclass, and calling it via bean proxies will result is a null
                  // value leading to a subsequent NullPointerException.
                  return anonymous();
                }
              }

              return Uni.createFrom().item(securityIdentity);
            });
  }

  private Uni<SecurityIdentity> anonymous() {
    return identityProvider.authenticate(AnonymousAuthenticationRequest.INSTANCE);
  }
}
