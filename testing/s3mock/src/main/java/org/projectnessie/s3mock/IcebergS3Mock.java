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
package org.projectnessie.s3mock;

import com.fasterxml.jackson.jakarta.rs.xml.JacksonXMLProvider;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.jetty.JettyHttpContainerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.immutables.value.Value;

@Value.Immutable
public abstract class IcebergS3Mock {

  @Value.Default
  public String initAddress() {
    return "127.0.0.1";
  }

  public static ImmutableIcebergS3Mock.Builder builder() {
    return ImmutableIcebergS3Mock.builder();
  }

  public abstract Map<String, S3Bucket> buckets();

  public interface S3MockServer extends AutoCloseable {
    URI getBaseUri();

    default Map<String, String> icebergProperties() {
      Map<String, String> props = new HashMap<>();
      props.put("s3.access-key-id", "accessKey");
      props.put("s3.secret-access-key", "secretKey");
      props.put("s3.endpoint", getBaseUri().toString());
      props.put("http-client.type", "urlconnection");
      return props;
    }
  }

  private static final class MockServer implements S3MockServer {

    private final Server server;

    private final URI baseUri;

    public MockServer(URI initUri, ResourceConfig config) {
      this.server = JettyHttpContainerFactory.createServer(initUri, config, true);
      this.baseUri = baseUri(server, initUri);
    }

    private static URI baseUri(Server server, URI initUri) {
      for (Connector connector : server.getConnectors()) {
        if (connector instanceof ServerConnector) {
          ServerConnector sc = (ServerConnector) connector;
          int localPort = sc.getLocalPort();
          try {
            return new URI(
                initUri.getScheme(),
                initUri.getUserInfo(),
                initUri.getHost(),
                localPort,
                initUri.getPath(),
                null,
                null);
          } catch (URISyntaxException e) {
            throw new RuntimeException(e);
          }
        }
      }

      throw new IllegalArgumentException("Server has no connectors");
    }

    @Override
    public URI getBaseUri() {
      return baseUri;
    }

    @Override
    public void close() throws Exception {
      if (server != null) {
        server.stop();
      }
    }
  }

  public S3MockServer start() {
    ResourceConfig config = new ResourceConfig();

    config.register(
        new AbstractBinder() {
          @Override
          protected void configure() {
            this.bind(IcebergS3Mock.this).to(IcebergS3Mock.class);
          }
        });
    config.register(JacksonXMLProvider.class);

    config.register(S3Resource.class);

    URI initUri = URI.create(String.format("http://%s:0/", initAddress()));

    return new MockServer(initUri, config);
  }
}
