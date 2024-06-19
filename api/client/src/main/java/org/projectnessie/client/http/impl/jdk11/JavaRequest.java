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
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpConnectTimeoutException;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublisher;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.net.http.HttpTimeoutException;
import java.nio.channels.Channels;
import java.nio.channels.Pipe;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Flow;
import java.util.concurrent.ForkJoinPool;
import java.util.function.BiConsumer;
import org.projectnessie.client.http.HttpClient.Method;
import org.projectnessie.client.http.HttpClientException;
import org.projectnessie.client.http.HttpClientReadTimeoutException;
import org.projectnessie.client.http.HttpClientResponseException;
import org.projectnessie.client.http.RequestContext;
import org.projectnessie.client.http.ResponseContext;
import org.projectnessie.client.http.Status;
import org.projectnessie.client.http.impl.BaseHttpRequest;
import org.projectnessie.client.http.impl.HttpHeaders.HttpHeader;
import org.projectnessie.client.http.impl.HttpRuntimeConfig;
import org.projectnessie.client.http.impl.RequestContextImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Implements Nessie HTTP request processing using Java's new {@link HttpClient}. */
@SuppressWarnings("Since15") // IntelliJ warns about new APIs. 15 is misleading, it means 11
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

  JavaRequest(HttpRuntimeConfig config, URI baseUri, HttpExchange<InputStream> exchange) {
    super(config, baseUri);
    this.exchange = exchange;
  }

  @Override
  public org.projectnessie.client.http.HttpResponse executeRequest(Method method, Object body)
      throws HttpClientException {

    URI uri = uriBuilder.build();

    HttpRequest.Builder request =
        HttpRequest.newBuilder().uri(uri).timeout(Duration.ofMillis(config.getReadTimeoutMillis()));

    RequestContext context = new RequestContextImpl(headers, uri, method, body);

    try {
      boolean doesOutput = prepareRequest(context);

      for (HttpHeader header : headers.allHeaders()) {
        for (String value : header.getValues()) {
          request = request.header(header.getName(), value);
        }
      }

      BodyPublisher bodyPublisher = doesOutput ? bodyPublisher(context) : BodyPublishers.noBody();
      request = request.method(method.name(), bodyPublisher);

      HttpResponse<InputStream> response = null;
      try {
        try {
          LOGGER.debug("Sending {} request to {} ...", method, uri);
          response = exchange.send(request.build(), BodyHandlers.ofInputStream());
        } catch (HttpConnectTimeoutException e) {
          throw new HttpClientException(
              String.format(
                  "Timeout connecting to '%s' after %ds",
                  uri, config.getConnectionTimeoutMillis() / 1000),
              e);
        } catch (HttpTimeoutException e) {
          throw new HttpClientReadTimeoutException(
              String.format(
                  "Cannot finish %s request against '%s'. Timeout while waiting for response with a timeout of %ds",
                  method, uri, config.getReadTimeoutMillis() / 1000),
              e);
        } catch (MalformedURLException e) {
          throw new HttpClientException(
              String.format("Cannot perform %s request. Malformed Url for %s", method, uri), e);
        } catch (IOException e) {
          throw new HttpClientException(
              String.format("Failed to execute %s request against '%s'.", method, uri), e);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }

        JavaResponseContext responseContext = new JavaResponseContext(response);

        List<BiConsumer<ResponseContext, Exception>> callbacks = context.getResponseCallbacks();
        if (callbacks != null) {
          callbacks.forEach(callback -> callback.accept(responseContext, null));
        }

        config
            .getResponseFilters()
            .forEach(responseFilter -> responseFilter.filter(responseContext));

        if (response.statusCode() >= 400) {
          // This mimics the (weird) behavior of java.net.HttpURLConnection.getResponseCode() that
          // throws an IOException for these status codes.
          Status status = Status.fromCode(response.statusCode());
          throw new HttpClientResponseException(
              String.format(
                  "%s request to %s failed with HTTP/%d", method, uri, response.statusCode()),
              status);
        }

        response = null;
        return config.responseFactory().make(responseContext, config.getMapper());
      } finally {
        if (response != null) {
          try {
            LOGGER.debug(
                "Closing unprocessed input stream for {} request to {} delegating to {} ...",
                method,
                uri,
                response.body());
            response.body().close();
          } catch (IOException e) {
            // ignore
          }
        }
      }
    } finally {
      cleanUp();
    }
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

  /**
   * Executor used to serialize the response object to JSON.
   *
   * <p>Java's new {@link HttpClient} uses the {@link Flow.Publisher}/{@link Flow.Subscriber}/{@link
   * Flow.Subscription} mechanism to write and read request and response data. We have to use that
   * protocol. Since none of the implementations must block, writes and reads run in a separate
   * pool.
   *
   * <p>Jackson has no "reactive" serialization mechanism, which means that we have to provide a
   * custom {@link OutputStream}, which delegates {@link Flow.Subscriber#onNext(Object) writes} to
   * the subscribing code.
   */
  private static final Executor writerPool =
      new ForkJoinPool(Math.max(8, ForkJoinPool.getCommonPoolParallelism()));
}
