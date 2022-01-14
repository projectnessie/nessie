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
package org.projectnessie.test.compatibility;

public final class Util {

  public static final String CLS_NAME_ABSTRACT_TEST_REST =
      "org.projectnessie.jaxrs.AbstractTestRest";
  public static final String CLS_NAME_ABSTRACT_TEST_RESTEASY =
      "org.projectnessie.jaxrs.AbstractResteasyTest";
  public static final String CLS_NAME_REST_ASSURED = "io.restassured.RestAssured";
  public static final String METHOD_UPDATE_TEST_SERVER_URI = "testServerUri";
  public static final String FIELD_NESSIE_VERSION = "nessieVersion";

  private Util() {}
}
