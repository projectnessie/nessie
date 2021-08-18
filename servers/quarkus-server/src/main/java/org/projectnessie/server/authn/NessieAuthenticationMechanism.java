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

import io.quarkus.oidc.runtime.OidcAuthenticationMechanism;
import io.quarkus.security.AuthenticationFailedException;
import io.quarkus.security.identity.IdentityProviderManager;
import io.quarkus.security.identity.SecurityIdentity;
import io.quarkus.security.identity.request.AuthenticationRequest;
import io.quarkus.vertx.http.runtime.security.BasicAuthenticationMechanism;
import io.quarkus.vertx.http.runtime.security.ChallengeData;
import io.quarkus.vertx.http.runtime.security.HttpAuthenticationMechanism;
import io.quarkus.vertx.http.runtime.security.HttpCredentialTransport;
import io.smallrye.mutiny.Uni;
import io.vertx.ext.web.RoutingContext;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Priority;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Alternative;
import javax.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/** TODO: javadoc. */
@Alternative
@Priority(1)
@ApplicationScoped
public class NessieAuthenticationMechanism implements HttpAuthenticationMechanism {

  @Inject OidcAuthenticationMechanism oidc;

  @Inject BasicAuthenticationMechanism basic;

  @ConfigProperty(name = "nessie.auth.enabled")
  boolean authEnabled;

  @ConfigProperty(name = "quarkus.http.auth.basic")
  boolean basicEnabled;

  @Override
  public Uni<SecurityIdentity> authenticate(
      RoutingContext context, IdentityProviderManager identityProviderManager) {
    if (authEnabled) {
      Uni<SecurityIdentity> uni = oidc.authenticate(context, identityProviderManager);

      if (basicEnabled) {
        uni =
            uni.onItem()
                .transformToUni(
                    securityIdentity -> {
                      if (securityIdentity == null) {
                        return basic.authenticate(context, identityProviderManager);
                      }

                      return Uni.createFrom().item(securityIdentity);
                    });
      }

      return uni.onItem()
          .transformToUni(
              securityIdentity -> {
                if (securityIdentity == null) {
                  return Uni.createFrom()
                      .failure(new AuthenticationFailedException("Not authenticated"));
                }

                return Uni.createFrom().item(securityIdentity);
              });
    }

    return Uni.createFrom().optional(Optional.empty());
  }

  @Override
  public Uni<ChallengeData> getChallenge(RoutingContext context) {
    return oidc.getChallenge(context);
  }

  @Override
  public Set<Class<? extends AuthenticationRequest>> getCredentialTypes() {
    return oidc.getCredentialTypes();
  }

  @Override
  public HttpCredentialTransport getCredentialTransport() {
    return oidc.getCredentialTransport();
  }
}
