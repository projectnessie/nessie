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
package org.projectnessie.client.auth;

import org.projectnessie.client.http.HttpAuthentication;

/**
 * Base interface for different authentication methods like "basic" (username + plain password),
 * bearer token, etc.
 *
 * <p>Implementations of this interface, as returned via {@link
 * NessieAuthenticationProvider#build(java.util.function.Function)}, do have to implement transport
 * specific implementations. For example, the Nessie HTTP based transport implements the {@link
 * HttpAuthentication}.
 */
public interface NessieAuthentication {}
