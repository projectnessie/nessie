/*
 * Copyright (C) 2022 Dremio
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
package org.projectnessie.client.http;

/**
 * This interface defines execution methods for HTTP client requests.
 *
 * @see HttpRequestWrapper
 */
public interface ExecutableHttpRequest<E1 extends Throwable, E2 extends Throwable> {

  HttpResponse get() throws E1, E2;

  HttpResponse delete() throws E1, E2;

  HttpResponse post(Object obj) throws E1, E2;

  HttpResponse put(Object obj) throws E1, E2;
}
