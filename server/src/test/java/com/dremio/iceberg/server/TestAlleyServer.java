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

package com.dremio.iceberg.server;

import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.ErrorHandler;
import org.eclipse.jetty.server.handler.RequestLogHandler;
import org.eclipse.jetty.server.handler.gzip.GzipHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.servlet.ServletContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestAlleyServer implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(TestAlleyServer.class);

  private final Server server;
  private ServletContextHandler servletContextHandler;
  private ServerConnector serverConnector;

  public TestAlleyServer() {
    server = new Server();

  }

  public void start(int port) throws Exception {
    serverConnector =
      new ServerConnector(server, new HttpConnectionFactory(new HttpConfiguration()));

    serverConnector.setPort(port);
    server.addConnector(serverConnector);

    final RequestLogHandler rootHandler = new RequestLogHandler();
    server.insertHandler(rootHandler);

    // gzip handler.
    final GzipHandler gzipHandler = new GzipHandler();
    rootHandler.setHandler(gzipHandler);

    // servlet handler for everything (to manage path mapping)
    servletContextHandler = new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
    servletContextHandler.setContextPath("/");
    gzipHandler.setHandler(servletContextHandler);

    // error handler
    final ErrorHandler errorHandler = new ErrorHandler();
    errorHandler.setShowStacks(true);
    errorHandler.setShowMessageInTitle(true);
    server.setErrorHandler(errorHandler);

    RestServerV1 restServer = new RestServerV1(new AlleyTestServerBinder());

    final ServletHolder restHolder = new ServletHolder(new ServletContainer(restServer));
    restHolder.setInitOrder(2);
    servletContextHandler.addServlet(restHolder, "/api/v1/*");

    server.start();

    logger.info("Started on http://localhost:19120");
  }

  @Override
  public void close() throws Exception {
    server.stop();
    server.destroy();
  }
}
