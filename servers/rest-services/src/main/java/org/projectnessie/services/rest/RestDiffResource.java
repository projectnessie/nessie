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

import static org.projectnessie.services.impl.RefUtil.toReference;
import static org.projectnessie.services.rest.RestApiContext.NESSIE_V1;

import com.fasterxml.jackson.annotation.JsonView;
import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.Path;
import org.projectnessie.api.v1.http.HttpDiffApi;
import org.projectnessie.api.v1.params.DiffParams;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.DiffResponse;
import org.projectnessie.model.DiffResponse.DiffEntry;
import org.projectnessie.model.ImmutableDiffResponse;
import org.projectnessie.model.ser.Views;
import org.projectnessie.services.authz.AccessContext;
import org.projectnessie.services.authz.Authorizer;
import org.projectnessie.services.config.ServerConfig;
import org.projectnessie.services.impl.DiffApiImpl;
import org.projectnessie.services.spi.DiffService;
import org.projectnessie.services.spi.PagedResponseHandler;
import org.projectnessie.versioned.VersionStore;

/** REST endpoint for the diff-API. */
@RequestScoped
@Path("api/v1/diffs")
public class RestDiffResource implements HttpDiffApi {
  // Cannot extend the DiffApiImplWithAuthz class, because then CDI gets confused
  // about which interface to use - either HttpContentApi or the plain ContentApi. This can lead
  // to various symptoms: complaints about varying validation-constraints in HttpTreeApi + TreeAPi,
  // empty resources (no REST methods defined) and potentially other.

  private final DiffService diffService;

  // Mandated by CDI 2.0
  public RestDiffResource() {
    this(null, null, null, null);
  }

  @Inject
  public RestDiffResource(
      ServerConfig config, VersionStore store, Authorizer authorizer, AccessContext accessContext) {
    this.diffService = new DiffApiImpl(config, store, authorizer, accessContext, NESSIE_V1);
  }

  private DiffService resource() {
    return diffService;
  }

  @Override
  @JsonView(Views.V1.class)
  public DiffResponse getDiff(DiffParams params) throws NessieNotFoundException {
    ImmutableDiffResponse.Builder builder = DiffResponse.builder();
    return resource()
        .getDiff(
            params.getFromRef(),
            params.getFromHashOnRef(),
            params.getToRef(),
            params.getToHashOnRef(),
            null,
            new PagedResponseHandler<>() {

              @Override
              public DiffResponse build() {
                return builder.build();
              }

              @Override
              public boolean addEntry(DiffEntry entry) {
                builder.addDiffs(entry);
                return true;
              }

              @Override
              public void hasMore(String pagingToken) {
                builder.isHasMore(true).token(pagingToken);
              }
            },
            h -> builder.effectiveFromReference(toReference(h)),
            h -> builder.effectiveToReference(toReference(h)),
            null,
            null,
            null,
            null,
            null);
  }
}
