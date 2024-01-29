/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.quarkus.config;

import io.smallrye.config.ConfigSourceInterceptor;
import io.smallrye.config.ConfigSourceInterceptorContext;
import io.smallrye.config.ConfigValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OpenTelemetryConfigSourceInterceptor implements ConfigSourceInterceptor {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(OpenTelemetryConfigSourceInterceptor.class);
  private static final int APPLICATION_PROPERTIES_CLASSPATH_ORDINAL = 250;

  @Override
  public ConfigValue getValue(ConfigSourceInterceptorContext context, String name) {
    ConfigValue configValue = context.proceed(name);
    if (configValue == null || configValue.getValue() == null) {
      return null;
    }
    if (name.equals("quarkus.otel.sdk.disabled")
        && configValue.getConfigSourceOrdinal() <= APPLICATION_PROPERTIES_CLASSPATH_ORDINAL
        && configValue.getValue().equals("false")) {
      // Check for a user-configured endpoint URL. If none is found, disable OpenTelemetry.
      ConfigValue tracesEndpoint = context.proceed("quarkus.otel.exporter.otlp.traces.endpoint");
      if (tracesEndpoint.getConfigSourceOrdinal() <= APPLICATION_PROPERTIES_CLASSPATH_ORDINAL) {
        tracesEndpoint = context.proceed("quarkus.otel.exporter.otlp.endpoint");
      }
      if (tracesEndpoint.getConfigSourceOrdinal() <= APPLICATION_PROPERTIES_CLASSPATH_ORDINAL) {
        LOGGER.info("No OpenTelemetry endpoint configured, disabling OpenTelemetry");
        LOGGER.info(
            "To enable OpenTelemetry, define a collector endpoint URL "
                + "using the property: quarkus.otel.exporter.otlp.traces.endpoint");
        configValue = configValue.withValue("true");
      } else {
        LOGGER.info(
            "Found OpenTelemetry collector endpoint URL: {} (from property: {}); enabling OpenTelemetry",
            tracesEndpoint.getValue(),
            tracesEndpoint.getName());
      }
    }
    return configValue;
  }
}
