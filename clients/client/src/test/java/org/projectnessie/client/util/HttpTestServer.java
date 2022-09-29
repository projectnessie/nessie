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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.function.Consumer;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.server.handler.gzip.GzipHandler;

/** A HTTP test server. */
public class HttpTestServer implements AutoCloseable {

  @FunctionalInterface
  public interface RequestHandler {
    void handle(HttpServletRequest request, HttpServletResponse response) throws IOException;
  }

  private final Server server;

  public HttpTestServer(String context, RequestHandler handler) throws Exception {
    this(context, handler, null);
  }

  /**
   * Constructor.
   *
   * @param context server context
   * @param handler http request handler
   * @param init init method (optional)
   */
  public HttpTestServer(String context, RequestHandler handler, Consumer<Server> init)
      throws Exception {
    server = new Server(0);

    AbstractHandler requestHandler =
        new AbstractHandler() {
          @Override
          public void handle(
              String target,
              Request baseRequest,
              HttpServletRequest request,
              HttpServletResponse response)
              throws IOException {
            if (target.startsWith(context)) {
              handler.handle(request, response);
            }
          }
        };

    GzipHandler gzip = new GzipHandler();
    gzip.setInflateBufferSize(8192);
    gzip.addIncludedMethods("PUT");
    server.setHandler(requestHandler);
    server.insertHandler(gzip);

    if (init != null) {
      init.accept(server);
    }

    server.start();
  }

  public HttpTestServer(RequestHandler handler) throws Exception {
    this("/", handler);
  }

  public InetSocketAddress getAddress() {
    URI uri = server.getURI();
    return InetSocketAddress.createUnresolved("localhost", uri.getPort());
  }

  public URI getUri() {
    return URI.create("http://localhost:" + getAddress().getPort() + "/");
  }

  @Override
  public void close() throws Exception {
    server.stop();
  }
}
