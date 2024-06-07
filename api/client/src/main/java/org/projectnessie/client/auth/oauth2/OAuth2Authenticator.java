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
package org.projectnessie.client.auth.oauth2;

public interface OAuth2Authenticator extends AutoCloseable {

  /**
   * Starts the authenticator. This will trigger the first background authentication attempt,and the
   * result of this attempt will be returned by the next call to {@link #authenticate()}. This will
   * also start the periodic background refresh of the access token.
   *
   * <p>Calling this method on an already started authenticator has no effect.
   *
   * <p>This method never throws an exception.
   */
  void start();

  /**
   * Attempts to authenticate the client and returns a valid {@link AccessToken}.
   *
   * <p>This method may block until the authenticator is started and the first authentication
   * attempt is successful.
   *
   * @return a valid {@link AccessToken}
   * @throws OAuth2Exception if the authentication attempt fails
   * @throws org.projectnessie.client.http.HttpClientException if the authentication attempt fails
   *     due to a network error
   */
  AccessToken authenticate();

  /**
   * Closes the authenticator and releases all resources. This will interrupt any threads waiting on
   * {@link #authenticate()}, stop the periodic background refresh of the access token, and close
   * the underlying HTTP client and the internal Executor (unless the executor was user-provided).
   */
  @Override
  void close();

  OAuth2Authenticator copy();
}
