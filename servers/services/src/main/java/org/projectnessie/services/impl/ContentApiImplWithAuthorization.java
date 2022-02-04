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

import java.security.Principal;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.model.Content.Type;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.GetMultipleContentsRequest;
import org.projectnessie.model.GetMultipleContentsResponse;
import org.projectnessie.services.authz.Authorizer;
import org.projectnessie.services.authz.BatchAccessChecker;
import org.projectnessie.services.config.ServerConfig;
import org.projectnessie.versioned.NamedRef;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.WithHash;

/** Does authorization checks (if enabled) on the {@link ContentApiImpl}. */
public class ContentApiImplWithAuthorization extends ContentApiImpl {

  public ContentApiImplWithAuthorization(
      ServerConfig config,
      VersionStore<Content, CommitMeta, Type> store,
      Authorizer authorizer,
      Principal principal) {
    super(config, store, authorizer, principal);
  }

  @Override
  public Content getContent(ContentKey key, String namedRef, String hashOnRef)
      throws NessieNotFoundException {
    NamedRef ref = namedRefWithHashOrThrow(namedRef, hashOnRef).getValue();
    startAccessCheck().canReadEntityValue(ref, key, null).checkAndThrow();
    return super.getContent(key, namedRef, hashOnRef);
  }

  @Override
  public GetMultipleContentsResponse getMultipleContents(
      String namedRef, String hashOnRef, GetMultipleContentsRequest request)
      throws NessieNotFoundException {
    WithHash<NamedRef> ref = namedRefWithHashOrThrow(namedRef, hashOnRef);
    BatchAccessChecker check = startAccessCheck();
    request.getRequestedKeys().forEach(k -> check.canReadEntityValue(ref.getValue(), k, null));
    check.checkAndThrow();
    return super.getMultipleContents(namedRef, hashOnRef, request);
  }
}
