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

package com.dremio.nessie;

import ch.qos.logback.classic.LoggerContext;
import com.codahale.metrics.jvm.BufferPoolMetricSet;
import com.codahale.metrics.jvm.ClassLoadingGaugeSet;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.codahale.metrics.jvm.ThreadStatesGaugeSet;
import com.codahale.metrics.logback.InstrumentedAppender;
import com.codahale.metrics.servlet.InstrumentedFilter;
import com.dremio.nessie.server.InstrumentationFilter;
import com.dremio.nessie.server.RestServerV1;
import com.dremio.nessie.server.ServerConfiguration;
import com.dremio.nessie.server.ServerConfigurationImpl;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.dropwizard.DropwizardExports;
import io.prometheus.client.exporter.MetricsServlet;
import java.io.Closeable;
import java.lang.management.ManagementFactory;
import java.util.EnumSet;
import javax.servlet.DispatcherType;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.ErrorHandler;
import org.eclipse.jetty.server.handler.RequestLogHandler;
import org.eclipse.jetty.server.handler.gzip.GzipHandler;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.resource.Resource;
import org.glassfish.jersey.servlet.ServletContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NessieServer implements Closeable {
  private static final Logger logger = LoggerFactory.getLogger(NessieServer.class);

  private final Server server;
  private ServletContextHandler servletContextHandler;
  private ServerConnector serverConnector;

  public NessieServer() {
    server = new Server();

  }

  public void start() throws Exception {
    serverConnector =
      new ServerConnector(server, new HttpConnectionFactory(new HttpConfiguration()));

    ServerConfiguration config = new ServerConfigurationImpl.ConfigurationFactory().provide();
    serverConnector.setPort(config.getServiceConfiguration().getPort());
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
    if (config.getServiceConfiguration().getMetrics()) {
      metrics();
    }

    // error handler
    final ErrorHandler errorHandler = new ErrorHandler();
    errorHandler.setShowStacks(true);
    errorHandler.setShowMessageInTitle(true);
    server.setErrorHandler(errorHandler);

    RestServerV1 restServer = new RestServerV1();

    final ServletHolder restHolder = new ServletHolder(new ServletContainer(restServer));
    restHolder.setInitOrder(2);
    servletContextHandler.addServlet(restHolder, "/api/v1/*");

    if (config.getServiceConfiguration().getSwagger()) {
      final String markerPath = "swagger-ui/index.html";
      ServletHolder holder = new ServletHolder("swagger-ui", DefaultServlet.class);
      addStaticPath(holder, "swagger-ui", markerPath);
      servletContextHandler.addServlet(holder, "/swagger-ui/*");
    }

    if (config.getServiceConfiguration().getUi()) {
      final String uiMarkerPath = "build/index.html";
      ServletHolder uiHolder = new ServletHolder("nessie-ui", DefaultServlet.class);
      addStaticPath(uiHolder, "build", uiMarkerPath);
      servletContextHandler.addServlet(uiHolder, "/*");
    }
    server.start();

    logger.info("Started on http://localhost:19120");
  }

  protected void addStaticPath(ServletHolder holder, String basePath,
                               String relativeMarkerPathToResource) {
    String path = Resource.newClassPathResource(relativeMarkerPathToResource).getURI().toString();
    final String fullBasePath =
        path.substring(0, path.length() - relativeMarkerPathToResource.length()) + basePath;
    holder.setInitParameter("dirAllowed", "false");
    holder.setInitParameter("pathInfoOnly", "true");
    holder.setInitParameter("resourceBase", fullBasePath);
  }

  private void metrics() {
    MetricsServlet metricsServlet = new MetricsServlet(CollectorRegistry.defaultRegistry);
    final ServletHolder metricsHolder = new ServletHolder(metricsServlet);
    metricsHolder.setInitOrder(3);

    final LoggerContext factory = (LoggerContext) LoggerFactory.getILoggerFactory();
    final ch.qos.logback.classic.Logger root = factory.getLogger(Logger.ROOT_LOGGER_NAME);

    final InstrumentedAppender metrics = new InstrumentedAppender(InstrumentationFilter.REGISTRY);
    metrics.setContext(root.getLoggerContext());
    metrics.start();
    root.addAppender(metrics);
    CollectorRegistry.defaultRegistry
      .register(new DropwizardExports(InstrumentationFilter.REGISTRY));
    InstrumentationFilter.REGISTRY.registerAll(new GarbageCollectorMetricSet());
    InstrumentationFilter.REGISTRY.registerAll(new MemoryUsageGaugeSet());
    InstrumentationFilter.REGISTRY.registerAll(new ClassLoadingGaugeSet());
    InstrumentationFilter.REGISTRY.registerAll(new ThreadStatesGaugeSet());
    InstrumentationFilter.REGISTRY
      .registerAll(new BufferPoolMetricSet(ManagementFactory.getPlatformMBeanServer()));

    servletContextHandler.addServlet(metricsHolder, "/metrics");
    servletContextHandler.addEventListener(new InstrumentationFilter());
    servletContextHandler.addFilter(new FilterHolder(new InstrumentedFilter()), "/*",
        EnumSet.of(DispatcherType.REQUEST, DispatcherType.ERROR));
  }

  public static void main(String[] args) {
    try {
      new NessieServer().start();
    } catch (Exception e) {
      logger.error("died", e);
    }
  }

  @Override
  public void close() {

  }
}
