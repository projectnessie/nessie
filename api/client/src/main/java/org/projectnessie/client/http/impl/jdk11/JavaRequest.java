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
package org.projectnessie.client.http.impl.jdk11;

import static java.lang.Thread.currentThread;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublisher;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.channels.Channels;
import java.nio.channels.Pipe;
import java.time.Duration;
import java.util.concurrent.Executor;
import org.projectnessie.client.http.HttpClient.Method;
import org.projectnessie.client.http.RequestContext;
import org.projectnessie.client.http.ResponseContext;
import org.projectnessie.client.http.impl.BaseHttpRequest;
import org.projectnessie.client.http.impl.HttpHeaders.HttpHeader;
import org.projectnessie.client.http.impl.HttpRuntimeConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Implements Nessie HTTP request processing using Java's new {@link HttpClient}. */
final class JavaRequest extends BaseHttpRequest {

  /**
   * A functional interface that is used to send an {@link HttpRequest} and return an {@link
   * HttpResponse} without leaking the {@link HttpClient} instance.
   */
  @FunctionalInterface
  interface HttpExchange<T> {

    /**
     * Sends the given request using the underlying client, blocking if necessary to get the
     * response. The returned {@link HttpResponse}{@code <T>} contains the response status, headers,
     * and body (as handled by given response body handler).
     *
     * @see HttpClient#send(HttpRequest, HttpResponse.BodyHandler)
     */
    HttpResponse<T> send(HttpRequest request, HttpResponse.BodyHandler<T> responseBodyHandler)
        throws IOException, InterruptedException;
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(JavaRequest.class);

  private final HttpExchange<InputStream> exchange;
  private final Executor writerPool;

  JavaRequest(
      HttpRuntimeConfig config,
      URI baseUri,
      HttpExchange<InputStream> exchange,
      Executor writerPool) {
    super(config, baseUri);
    this.exchange = exchange;
    this.writerPool = writerPool;
  }

  @Override
  protected ResponseContext sendAndReceive(
      URI uri, Method method, Object body, RequestContext requestContext)
      throws IOException, InterruptedException {

    HttpRequest.Builder request =
        HttpRequest.newBuilder().uri(uri).timeout(Duration.ofMillis(config.getReadTimeoutMillis()));

    for (HttpHeader header : headers.allHeaders()) {
      for (String value : header.getValues()) {
        request = request.header(header.getName(), value);
      }
    }

    BodyPublisher bodyPublisher =
        requestContext.doesOutput() ? bodyPublisher(requestContext) : BodyPublishers.noBody();
    request = request.method(method.name(), bodyPublisher);

    LOGGER.debug("Sending {} request to {} ...", method, uri);
    HttpResponse<InputStream> response =
        exchange.send(request.build(), BodyHandlers.ofInputStream());
    return new JavaResponseContext(response);
  }

  private BodyPublisher bodyPublisher(RequestContext context) {
    ClassLoader cl = getClass().getClassLoader();
    return BodyPublishers.ofInputStream(
        () -> {
          try {
            Pipe pipe = Pipe.open();

            writerPool.execute(
                () -> {
                  ClassLoader restore = currentThread().getContextClassLoader();
                  try {
                    // Okay - this is weird - but it is necessary when running tests with Quarkus
                    // via `./gradlew :nessie-quarkus:test`.
                    currentThread().setContextClassLoader(cl);

                    writeToOutputStream(context, Channels.newOutputStream(pipe.sink()));
                  } catch (Exception e) {
                    throw new RuntimeException(e);
                  } finally {
                    currentThread().setContextClassLoader(restore);
                  }
                });
            return Channels.newInputStream(pipe.source());
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        });
  }
}
