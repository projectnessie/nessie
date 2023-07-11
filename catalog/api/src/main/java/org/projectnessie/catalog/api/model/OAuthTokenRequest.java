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
package org.projectnessie.catalog.api.model;

import java.util.List;
import java.util.Map;
import org.immutables.value.Value;

/**
 * A request made to the OAuth token endpoint.
 *
 * <p>Since the OAuth token endpoint is meant to be implemented as a pass-through proxy that
 * forwards the request to the actual OAuth/OIDC provider, this class is designed to be a generic
 * representation of the original request and strives to parse it as little as possible.
 */
@Value.Immutable
public interface OAuthTokenRequest {

  /** The HTTP headers of the request. */
  Map<String, List<String>> headers();

  /** The raw request body, which is assumed to be form data. */
  byte[] body();
}
