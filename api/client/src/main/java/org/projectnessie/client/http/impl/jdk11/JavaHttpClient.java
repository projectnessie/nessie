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

import java.io.OutputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpClient.Redirect;
import java.net.http.HttpClient.Version;
import java.time.Duration;
import java.util.Locale;
import java.util.concurrent.Flow;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import org.projectnessie.client.http.HttpRequest;
import org.projectnessie.client.http.impl.HttpRuntimeConfig;

/**
 * Nessie's HTTP client used when running on Java 11 or newer.
 *
 * <p>Java's new {@link HttpClient} provides a lot of benefits, such as HTTP/2 support, proper
 * redirection support (if enabled), and advanced security capabilities. See the {@link
 * org.projectnessie.client.NessieClientBuilder} for parameters supported only by this client
 * implementation.
 */
final class JavaHttpClient implements org.projectnessie.client.http.HttpClient {
  final HttpRuntimeConfig config;
  private HttpClient client;

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
  private final ForkJoinPool writerPool;

  JavaHttpClient(HttpRuntimeConfig config) {
    this.config = config;

    HttpClient.Builder clientBuilder =
        HttpClient.newBuilder()
            .connectTimeout(Duration.ofMillis(config.getConnectionTimeoutMillis()));

    if (config.getSslContext() != null) {
      clientBuilder.sslContext(config.getSslContext());
    }
    if (config.getSslParameters() != null) {
      clientBuilder.sslParameters(config.getSslParameters());
    }

    if (config.getFollowRedirects() != null) {
      clientBuilder.followRedirects(
          Redirect.valueOf(config.getFollowRedirects().toUpperCase(Locale.ROOT)));
    }

    if (config.isHttp11Only()) {
      clientBuilder.version(Version.HTTP_1_1);
    }

    client = clientBuilder.build();
    writerPool = new ForkJoinPool(Math.max(8, ForkJoinPool.getCommonPoolParallelism()));
  }

  @Override
  public HttpRequest newRequest(URI baseUri) {
    return new JavaRequest(this.config, baseUri, client::send, writerPool);
  }

  @Override
  public URI getBaseUri() {
    return config.getBaseUri();
  }

  @SuppressWarnings({"DataFlowIssue", "ResultOfMethodCallIgnored"})
  @Override
  public void close() {
    Exception fail = null;
    try {
      if (client instanceof AutoCloseable) {
        ((AutoCloseable) client).close();
      }
    } catch (Exception e) {
      fail = e;
    } finally {
      client = null;
    }

    try {
      config.close();
    } catch (Exception e) {
      if (fail == null) {
        fail = e;
      } else {
        fail.addSuppressed(e);
      }
    } finally {
      try {
        writerPool.shutdown();
        try {
          writerPool.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          // populate
          Thread.currentThread().interrupt();
        }
      } catch (Exception e) {
        if (fail == null) {
          fail = e;
        } else {
          fail.addSuppressed(e);
        }
      }
    }

    if (fail instanceof RuntimeException) {
      throw (RuntimeException) fail;
    }
    if (fail != null) {
      throw new RuntimeException(fail);
    }
  }
}
