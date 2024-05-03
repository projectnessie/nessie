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
package org.projectnessie.server;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.restassured.RestAssured;
import io.restassured.specification.RequestSpecification;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@QuarkusTest
@TestProfile(value = TestBasicAuthentication.Profile.class)
public class TestHealthCheckNoAuthentication {

  private static RequestSpecification request() {
    RestAssured.port = Integer.getInteger("quarkus.management.port");
    return given().when().baseUri(RestAssured.baseURI);
  }

  public static Stream<Arguments> paths() {
    return Stream.of("/q/health/live", "/q/health/live/", "/q/health/ready", "/q/health/ready/")
        .map(Arguments::of);
  }

  @ParameterizedTest
  @MethodSource("paths")
  void testValidCredentials(String path) {
    assertThat(request().basePath(path).auth().basic("test_user", "test_user").get().statusCode())
        .isEqualTo(200);
  }

  @ParameterizedTest
  @MethodSource("paths")
  void testNoCredentials(String path) {
    assertThat(request().basePath(path).get().statusCode()).isEqualTo(200);
  }
}
