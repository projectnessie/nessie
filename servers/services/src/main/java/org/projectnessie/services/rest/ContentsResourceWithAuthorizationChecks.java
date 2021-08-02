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

import javax.enterprise.context.RequestScoped;
import javax.enterprise.inject.Default;
import javax.inject.Inject;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Contents;
import org.projectnessie.model.Contents.Type;
import org.projectnessie.model.ContentsKey;
import org.projectnessie.model.MultiGetContentsRequest;
import org.projectnessie.model.MultiGetContentsResponse;
import org.projectnessie.services.authz.AccessChecker;
import org.projectnessie.services.config.ServerConfig;
import org.projectnessie.versioned.NamedRef;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.WithHash;

/** Does authorization checks (if enabled) on the {@link ContentsResource} endpoints. */
@RequestScoped
@Default
public class ContentsResourceWithAuthorizationChecks extends ContentsResource {

  @Inject
  public ContentsResourceWithAuthorizationChecks(
      ServerConfig config,
      MultiTenant multiTenant,
      VersionStore<Contents, CommitMeta, Type> store,
      AccessChecker accessChecker) {
    super(config, multiTenant, store, accessChecker);
  }

  @Override
  public Contents getContents(ContentsKey key, String namedRef, String hashOnRef)
      throws NessieNotFoundException {
    NamedRef ref = namedRefWithHashOrThrow(namedRef, hashOnRef).getValue();
    getAccessChecker().canReadEntityValue(createAccessContext(), ref, key, null);
    return super.getContents(key, namedRef, hashOnRef);
  }

  @Override
  public MultiGetContentsResponse getMultipleContents(
      String namedRef, String hashOnRef, MultiGetContentsRequest request)
      throws NessieNotFoundException {
    WithHash<NamedRef> ref = namedRefWithHashOrThrow(namedRef, hashOnRef);
    request
        .getRequestedKeys()
        .forEach(
            k ->
                getAccessChecker()
                    .canReadEntityValue(createAccessContext(), ref.getValue(), k, null));
    return super.getMultipleContents(namedRef, hashOnRef, request);
  }
}
