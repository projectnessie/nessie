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
package org.projectnessie.services.rest;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.inject.Inject;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.PreMatching;
import javax.ws.rs.core.UriInfo;
import javax.ws.rs.ext.Provider;
import org.projectnessie.model.Validation;

/**
 * Extracts the optional repository owner+name URI path parts and configured the request-scoped
 * {@link MultiTenant} bean accordingly.
 *
 * <p>Non-multi-tenant URIs just do not have owner+repo information in the URI.
 *
 * <p>Multi-tenant URIs have the owner+repo parts after the {@code /api/v1} URI path part, for
 * example: {@code http://localhost:19120/api/v1/owner/repo/trees/tree}.
 */
@PreMatching
@Provider
public class MultiTenantFilter implements ContainerRequestFilter {

  private static final Pattern MULTI_TENANT_PATTERN =
      Pattern.compile("/(\\w+)/(\\w+)/((trees|contents|config).*)");

  @Inject MultiTenant multiTenant;

  @Override
  public void filter(ContainerRequestContext requestContext) throws IOException {
    UriInfo uriInfo = requestContext.getUriInfo();
    URI baseUri = uriInfo.getBaseUri();
    URI reqUri = requestContext.getUriInfo().getRequestUri();

    // contains something like '/api/v1/'
    String basePath = baseUri.getPath();
    // contains something like '/api/v1/trees/tree' or '/api/v1/robert/elani/trees/tree'
    String reqPath = reqUri.getPath();

    String ctxPath = reqPath.substring(basePath.length() - 1); // -1 to include the trailing slash
    Matcher m = MULTI_TENANT_PATTERN.matcher(ctxPath);
    if (m.matches()) {
      // If the request-URI is a multi-tenant URI with /owner/repo after the REST base path,
      // continue with that /owner/repo part removed from the request URI and set owner + repo
      // as attributes.
      String owner = m.group(1);
      String repo = m.group(2);

      if (Validation.isValidOwner(owner) && Validation.isValidRepo(repo)) {
        // 'rest' contains the API endpoint without the base-path, without the owner+repo
        // and without a leading slash.
        String rest = m.group(3);
        multiTenant.setOwnerAndRepo(owner, repo);
        String path = basePath + rest;
        try {
          URI newUri =
              new URI(
                  reqUri.getScheme(),
                  reqUri.getUserInfo(),
                  reqUri.getHost(),
                  reqUri.getPort(),
                  path,
                  reqUri.getQuery(),
                  reqUri.getFragment());
          requestContext.setRequestUri(newUri);
        } catch (URISyntaxException e) {
          throw new IOException(e);
        }
      }
    }
  }
}
