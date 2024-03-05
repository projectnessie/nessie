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
package org.projectnessie.client.http;

import static org.projectnessie.client.NessieConfigConstants.CONF_FORCE_URL_CONNECTION_CLIENT;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_HTTP_2;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_HTTP_REDIRECT;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.util.function.Function;
import org.projectnessie.client.NessieClientBuilder;
import org.projectnessie.client.NessieConfigConstants;
import org.projectnessie.client.config.NessieClientConfigSources;

/** HTTP specific Nessie client builder interface. */
public interface NessieHttpClientBuilder extends NessieClientBuilder {

  /**
   * Whether to allow HTTP/2 upgrade, default is {@code false}.
   *
   * <p>Only valid on Java 11 and newer with Java's new HTTP client.
   */
  @CanIgnoreReturnValue
  NessieHttpClientBuilder withHttp2Upgrade(boolean http2Upgrade);

  /**
   * Whether HTTP redirects are followed, default is to not follow redirects.
   *
   * <p>Valid values are the enum names of {@link java.net.http.HttpClient.Redirect}.
   *
   * <p>Only valid on Java 11 and newer with Java's new HTTP client.
   */
  @CanIgnoreReturnValue
  NessieHttpClientBuilder withFollowRedirects(String redirects);

  /**
   * Whether to force using the "old" {@link java.net.URLConnection} based client when running on
   * Java 11 and newer with Java's new HTTP client.
   */
  @CanIgnoreReturnValue
  NessieHttpClientBuilder withForceUrlConnectionClient(boolean forceUrlConnectionClient);

  @CanIgnoreReturnValue
  NessieHttpClientBuilder withResponseFactory(HttpResponseFactory responseFactory);

  /**
   * Add a request filter to the client. Enables low-level access to the request/response
   * processing.
   */
  @CanIgnoreReturnValue
  NessieHttpClientBuilder addRequestFilter(RequestFilter filter);

  /**
   * Add a response filter to the client. Enables low-level access to the request/response
   * processing.
   */
  @CanIgnoreReturnValue
  NessieHttpClientBuilder addResponseFilter(ResponseFilter filter);

  /** Convenience base class for implementations of {@link NessieHttpClientBuilder}. */
  abstract class AbstractNessieHttpClientBuilder
      extends NessieClientBuilder.AbstractNessieClientBuilder implements NessieHttpClientBuilder {

    /**
     * Configure this HttpClientBuilder instance using a configuration object and standard Nessie
     * configuration keys defined by the constants defined in {@link NessieConfigConstants}.
     * Non-{@code null} values returned by the {@code configuration}-function will override
     * previously configured values.
     *
     * @param configuration The function that returns a configuration value for a configuration key.
     * @return {@code this}
     * @see NessieClientConfigSources
     */
    @Override
    public NessieHttpClientBuilder fromConfig(Function<String, String> configuration) {
      super.fromConfig(configuration);

      String s = configuration.apply(CONF_NESSIE_HTTP_2);
      if (s != null) {
        withHttp2Upgrade(Boolean.parseBoolean(s.trim()));
      }

      s = configuration.apply(CONF_NESSIE_HTTP_REDIRECT);
      if (s != null) {
        withFollowRedirects(s.trim());
      }

      s = configuration.apply(CONF_FORCE_URL_CONNECTION_CLIENT);
      if (s != null) {
        withForceUrlConnectionClient(Boolean.parseBoolean(s.trim()));
      }

      return this;
    }

    @Override
    public NessieHttpClientBuilder withHttp2Upgrade(boolean http2Upgrade) {
      return this;
    }

    @Override
    public NessieHttpClientBuilder withFollowRedirects(String redirects) {
      return this;
    }

    @Override
    public NessieHttpClientBuilder withForceUrlConnectionClient(boolean forceUrlConnectionClient) {
      return this;
    }

    @Override
    public NessieHttpClientBuilder withResponseFactory(HttpResponseFactory responseFactory) {
      return this;
    }

    @Override
    public NessieHttpClientBuilder addRequestFilter(RequestFilter filter) {
      return this;
    }

    @Override
    public NessieHttpClientBuilder addResponseFilter(ResponseFilter filter) {
      return this;
    }
  }
}
