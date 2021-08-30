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

import static org.assertj.core.api.Assertions.assertThat;

import io.jaegertracing.internal.JaegerTracer;
import io.opentracing.util.GlobalTracer;

public class JaegerTestTracer {
  public static void register() {
    if (GlobalTracer.isRegistered()) {
      // if already registered, check that it's the right tracer (and no other test accidentally
      // registered a different one)
      assertThat(GlobalTracer.get().toString())
          .contains("serviceName=TestNessieHttpClient")
          .contains("JaegerTracer");
    }
    GlobalTracer.registerIfAbsent(new JaegerTracer.Builder("TestNessieHttpClient").build());
  }
}
