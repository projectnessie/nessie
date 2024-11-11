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
package org.projectnessie.catalog.files.s3;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.catalog.files.api.StorageLocations;
import org.projectnessie.catalog.files.config.ImmutableS3ClientIam;
import org.projectnessie.catalog.files.config.S3ClientIam;
import org.projectnessie.storage.uri.StorageUri;

@ExtendWith(SoftAssertionsExtension.class)
class TestS3IamPolicies {

  @InjectSoftAssertions protected SoftAssertions soft;

  @Test
  void multipleStorageLocations() throws Exception {
    S3ClientIam clientIam =
        ImmutableS3ClientIam.builder()
            .enabled(true)
            .statements(
                List.of(
                    "{\"Effect\":\"Deny\", \"Action\":\"s3:*\", \"Resource\":\"arn:aws:s3:::*/blocked\\\"Namespace/*\"}"))
            .build();

    StorageLocations locations =
        StorageLocations.storageLocations(
            StorageUri.of("s3://bucket1/"),
            List.of(
                StorageUri.of("s3://bucket1/my/path/bar"),
                StorageUri.of("s3://bucket2/my/other/bar")),
            List.of(
                StorageUri.of("s3://bucket3/read/path/bar"),
                StorageUri.of("s3://bucket4/read/other/bar")));

    String policy = S3IamPolicies.locationDependentPolicy(clientIam, locations);

    String pretty = new ObjectMapper().readValue(policy, JsonNode.class).toPrettyString();

    soft.assertThat(pretty)
        .isEqualTo(
            "{\n"
                + "  \"Version\" : \"2012-10-17\",\n"
                + "  \"Statement\" : [ {\n"
                + "    \"Effect\" : \"Allow\",\n"
                + "    \"Action\" : \"s3:ListBucket\",\n"
                + "    \"Resource\" : \"arn:aws:s3:::bucket1\",\n"
                + "    \"Condition\" : {\n"
                + "      \"StringLike\" : {\n"
                + "        \"s3:prefix\" : [ \"my/path/bar\", \"my/path/bar/*\", \"*/my/path/bar\", \"*/my/path/bar/*\", \"*/*/*/*/my/path/bar\", \"*/*/*/*/my/path/bar/*\" ]\n"
                + "      }\n"
                + "    }\n"
                + "  }, {\n"
                + "    \"Effect\" : \"Allow\",\n"
                + "    \"Action\" : \"s3:ListBucket\",\n"
                + "    \"Resource\" : \"arn:aws:s3:::bucket2\",\n"
                + "    \"Condition\" : {\n"
                + "      \"StringLike\" : {\n"
                + "        \"s3:prefix\" : [ \"my/other/bar\", \"my/other/bar/*\", \"*/my/other/bar\", \"*/my/other/bar/*\", \"*/*/*/*/my/other/bar\", \"*/*/*/*/my/other/bar/*\" ]\n"
                + "      }\n"
                + "    }\n"
                + "  }, {\n"
                + "    \"Effect\" : \"Allow\",\n"
                + "    \"Action\" : \"s3:ListBucket\",\n"
                + "    \"Resource\" : \"arn:aws:s3:::bucket3\",\n"
                + "    \"Condition\" : {\n"
                + "      \"StringLike\" : {\n"
                + "        \"s3:prefix\" : [ \"read/path/bar\", \"read/path/bar/*\", \"*/read/path/bar\", \"*/read/path/bar/*\", \"*/*/*/*/read/path/bar\", \"*/*/*/*/read/path/bar/*\" ]\n"
                + "      }\n"
                + "    }\n"
                + "  }, {\n"
                + "    \"Effect\" : \"Allow\",\n"
                + "    \"Action\" : \"s3:ListBucket\",\n"
                + "    \"Resource\" : \"arn:aws:s3:::bucket4\",\n"
                + "    \"Condition\" : {\n"
                + "      \"StringLike\" : {\n"
                + "        \"s3:prefix\" : [ \"read/other/bar\", \"read/other/bar/*\", \"*/read/other/bar\", \"*/read/other/bar/*\", \"*/*/*/*/read/other/bar\", \"*/*/*/*/read/other/bar/*\" ]\n"
                + "      }\n"
                + "    }\n"
                + "  }, {\n"
                + "    \"Effect\" : \"Allow\",\n"
                + "    \"Action\" : [ \"s3:GetObject\", \"s3:GetObjectVersion\", \"s3:PutObject\", \"s3:DeleteObject\" ],\n"
                + "    \"Resource\" : [ \"arn:aws:s3:::bucket1/my/path/bar/*\", \"arn:aws:s3:::bucket1/*/my/path/bar/*\", \"arn:aws:s3:::bucket1/*/*/*/*/my/path/bar/*\", \"arn:aws:s3:::bucket2/my/other/bar/*\", \"arn:aws:s3:::bucket2/*/my/other/bar/*\", \"arn:aws:s3:::bucket2/*/*/*/*/my/other/bar/*\" ]\n"
                + "  }, {\n"
                + "    \"Effect\" : \"Allow\",\n"
                + "    \"Action\" : [ \"s3:GetObject\", \"s3:GetObjectVersion\" ],\n"
                + "    \"Resource\" : [ \"arn:aws:s3:::bucket3read/path/bar/*\", \"arn:aws:s3:::bucket3/*/read/path/bar/*\", \"arn:aws:s3:::bucket3/*/*/*/*/read/path/bar/*\", \"arn:aws:s3:::bucket4read/other/bar/*\", \"arn:aws:s3:::bucket4/*/read/other/bar/*\", \"arn:aws:s3:::bucket4/*/*/*/*/read/other/bar/*\" ]\n"
                + "  }, {\n"
                + "    \"Effect\" : \"Deny\",\n"
                + "    \"Action\" : \"s3:*\",\n"
                + "    \"Resource\" : \"arn:aws:s3:::*/blocked\\\"Namespace/*\"\n"
                + "  } ]\n"
                + "}");
  }

  @ParameterizedTest
  @MethodSource
  void locationDependentPolicy(S3ClientIam iam, List<String> expectedResources) {
    StorageUri location = StorageUri.of("s3://foo/b\"ar");
    StorageLocations locations =
        StorageLocations.storageLocations(StorageUri.of("s3://foo/"), List.of(location), List.of());
    soft.assertThatCode(
            () -> {
              String policy = S3IamPolicies.locationDependentPolicy(iam, locations);
              ObjectNode json = new ObjectMapper().readValue(policy, ObjectNode.class);
              ArrayNode statements = json.withArray("Statement");
              List<String> resources = new ArrayList<>();
              for (JsonNode statement : statements) {
                JsonNode res = statement.get("Resource");
                if (res.isArray()) {
                  for (JsonNode re : res) {
                    resources.add(re.asText());
                  }
                }
                if (res.isTextual()) {
                  resources.add(res.asText());
                }
              }
              assertThat(resources).containsExactlyElementsOf(expectedResources);
            })
        .doesNotThrowAnyException();
  }

  static Stream<Arguments> locationDependentPolicy() {
    return Stream.of(
        arguments(
            ImmutableS3ClientIam.builder().enabled(true).build(),
            List.of(
                "arn:aws:s3:::foo",
                "arn:aws:s3:::foo/b\"ar/*",
                "arn:aws:s3:::foo/*/b\"ar/*",
                "arn:aws:s3:::foo/*/*/*/*/b\"ar/*")),
        arguments(
            ImmutableS3ClientIam.builder()
                .enabled(true)
                .statements(
                    List.of(
                        "{\"Effect\":\"Deny\", \"Action\":\"s3:*\", \"Resource\":\"arn:aws:s3:::*/blocked\\\"Namespace/*\"}\n"))
                .build(),
            List.of(
                "arn:aws:s3:::foo",
                "arn:aws:s3:::foo/b\"ar/*",
                "arn:aws:s3:::foo/*/b\"ar/*",
                "arn:aws:s3:::foo/*/*/*/*/b\"ar/*",
                "arn:aws:s3:::*/blocked\"Namespace/*")));
  }

  @ParameterizedTest
  @MethodSource
  void invalidSessionPolicyStatement(String invalid) {
    StorageUri location = StorageUri.of("s3://foo/bar");
    StorageLocations locations =
        StorageLocations.storageLocations(StorageUri.of("s3://foo/"), List.of(location), List.of());
    S3ClientIam iam =
        ImmutableS3ClientIam.builder().enabled(true).statements(List.of(invalid)).build();
    soft.assertThatThrownBy(() -> S3IamPolicies.locationDependentPolicy(iam, locations))
        .isInstanceOf(RuntimeException.class)
        .cause()
        .isInstanceOf(IOException.class);
  }

  static Stream<String> invalidSessionPolicyStatement() {
    return Stream.of(
        "\"Effect\":\"Deny\", \"Action\":\"s3:*\", \"Resource\":\"arn:aws:s3:::*/blockedNamespace/*\"}",
        "\"Effect:\"Deny\", \"Action\":\"s3:*\", \"Resource\":\"arn:aws:s3:::*/blockedNamespace/*\"}",
        "}\"Effect:\"Deny\", \"Action\":\"s3:*\", \"Resource\":\"arn:aws:s3:::*/blockedNamespace/*\"}");
  }
}
