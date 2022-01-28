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
import io.quarkus.vertx.http.runtime.security.PathMatchingHttpSecurityPolicy;
import io.smallrye.mutiny.Uni;
import io.vertx.ext.web.RoutingContext;
import java.lang.annotation.Annotation;
import java.util.Collections;
import java.util.Iterator;
import javax.annotation.Priority;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Alternative;
import javax.enterprise.inject.Instance;
import javax.enterprise.util.TypeLiteral;
import javax.inject.Inject;
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
@ApplicationScoped
public class NessieHttpAuthenticator extends HttpAuthenticator {

  private final IdentityProviderManager identityProvider;
  private final boolean authEnabled;

  // Demanded by Quarkus/Arc
  public NessieHttpAuthenticator() {
    super(null, null, new EmptyInstance<>(), null);
    this.identityProvider = null;
    this.authEnabled = false;
  }

  // Only used to provide the empty constructor above and let the code not fail.
  static class EmptyInstance<T> implements Instance<T> {
    @Override
    public Instance<T> select(Annotation... qualifiers) {
      return null;
    }

    @Override
    public <U extends T> Instance<U> select(Class<U> subtype, Annotation... qualifiers) {
      return null;
    }

    @Override
    public <U extends T> Instance<U> select(TypeLiteral<U> subtype, Annotation... qualifiers) {
      return null;
    }

    @Override
    public boolean isUnsatisfied() {
      return false;
    }

    @Override
    public boolean isAmbiguous() {
      return false;
    }

    @Override
    public void destroy(T instance) {}

    @Override
    public Iterator<T> iterator() {
      return Collections.emptyListIterator();
    }

    @Override
    public T get() {
      return null;
    }
  }

  @Inject
  public NessieHttpAuthenticator(
      QuarkusNessieAuthenticationConfig config,
      IdentityProviderManager identityProviderManager,
      Instance<PathMatchingHttpSecurityPolicy> pathMatchingPolicy,
      Instance<HttpAuthenticationMechanism> httpAuthenticationMechanism,
      Instance<IdentityProvider<?>> providers) {
    super(identityProviderManager, pathMatchingPolicy, httpAuthenticationMechanism, providers);
    this.identityProvider = identityProviderManager;
    this.authEnabled = config.enabled();
  }

  @Override
  public Uni<SecurityIdentity> attemptAuthentication(RoutingContext context) {
    if (!authEnabled) {
      return anonymous();
    }

    return super.attemptAuthentication(context)
        .onItem()
        .transform(
            securityIdentity -> {
              if (securityIdentity == null) {
                // Disallow unauthenticated requests when requested by configuration.
                // Note: Quarkus by default permits unauthenticated requests unless there are
                // specific authorization rules that validate the security identity.
                throw new AuthenticationFailedException("Missing or unrecognized credentials");
              }

              return securityIdentity;
            });
  }

  private Uni<SecurityIdentity> anonymous() {
    return identityProvider.authenticate(AnonymousAuthenticationRequest.INSTANCE);
  }
}
