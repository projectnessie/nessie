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
package org.projectnessie.client.util;

import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpsServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.function.Consumer;

/** A HTTP test server. */
public class TestServer implements AutoCloseable {

  private final HttpServer server;

  public TestServer(String context, HttpHandler handler) throws IOException {
    this(context, handler, null);
  }

  /**
   * Constructor.
   *
   * @param context server context
   * @param handler http request handler
   * @param init init method (optional)
   * @throws IOException maybe
   */
  public TestServer(String context, HttpHandler handler, Consumer<HttpsServer> init)
      throws IOException {
    HttpHandler safeHandler =
        exchange -> {
          try {
            handler.handle(exchange);
          } catch (RuntimeException | Error e) {
            exchange.sendResponseHeaders(503, 0);
            throw e;
          }
        };
    if (init == null) {
      server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    } else {
      server = HttpsServer.create(new InetSocketAddress("localhost", 0), 0);
      init.accept((HttpsServer) server);
    }
    server.createContext(context, safeHandler);
    server.setExecutor(null);

    server.start();
  }

  public TestServer(HttpHandler handler) throws IOException {
    this("/", handler);
  }

  public InetSocketAddress getAddress() {
    return server.getAddress();
  }

  public URI getUri() {
    return URI.create(
        "http://"
            + getAddress().getAddress().getHostAddress()
            + ":"
            + getAddress().getPort()
            + "/");
  }

  @Override
  public void close() {
    server.stop(0);
  }
}
