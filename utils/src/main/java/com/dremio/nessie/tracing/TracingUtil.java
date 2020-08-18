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

package com.dremio.nessie.tracing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.jaegertracing.Configuration;
import io.jaegertracing.Configuration.ReporterConfiguration;
import io.jaegertracing.Configuration.SamplerConfiguration;
import io.jaegertracing.internal.samplers.RateLimitingSampler;
import io.opentracing.util.GlobalTracer;

/**
 * Utilities for Tracing config.
 */
public final class TracingUtil {

  private static final Logger logger = LoggerFactory.getLogger(TracingUtil.class);

  private TracingUtil() {

  }

  /**
   * If possible initialize Jaeger tracer.
   *
   * <p>
   * If Jaeger isn't present on classpath or some other issue with config this will fail gracefully with a warning
   * </p>
   *
   * @param service name of service as identified in Jaeger UI
   */
  public static void initTracer(String service) {

    SamplerConfiguration samplerConfig = SamplerConfiguration.fromEnv();
    //if sampler is undefined in environment use a 1sec rate limiter for QOS
    //env should be set to JAEGER_SAMPLER_TYPE="const" and JAEGER_SAMPLER_PARAM=1 for perf testing
    if (samplerConfig.getType() == null) {
      samplerConfig = samplerConfig.withType(RateLimitingSampler.TYPE);
    }
    if (samplerConfig.getParam() == null) {
      samplerConfig = samplerConfig.withParam(1);
    }
    ReporterConfiguration reporterConfig = ReporterConfiguration.fromEnv().withLogSpans(true);
    Configuration config = new Configuration(service).withSampler(samplerConfig)
                                                     .withReporter(reporterConfig);
    try {
      GlobalTracer.register(config.getTracer());
    } catch (RuntimeException t) {
      logger.warn("Unable to register Jaeger Tracing", t);
    }
  }

}
