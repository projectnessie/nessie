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
package org.projectnessie.catalog.files.config;

import java.io.IOException;
import software.amazon.awssdk.policybuilder.iam.IamPolicyReader;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.MappingIterator;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

final class S3IamValidation {
  private static final ObjectMapper MAPPER = JsonMapper.builder().build();
  private static final IamPolicyReader IAM_POLICY_READER = IamPolicyReader.create();

  private S3IamValidation() {}

  static void validateIam(S3Iam iam, String bucketName) {
    String root = iam instanceof S3ClientIam ? "client-iam" : "server-iam";
    if (iam.policy().isPresent()) {
      try {
        IAM_POLICY_READER.read(iam.policy().get());
      } catch (Exception e) {
        throw new IllegalStateException(
            "The "
                + root
                + ".policy"
                + " option for the "
                + bucketName
                + " bucket contains an invalid policy",
            e);
      }
    }
  }

  static void validateClientIam(S3ClientIam iam, String bucketName) {
    validateIam(iam, bucketName);
    try {
      iam.statements().ifPresent(stmts -> stmts.forEach(S3IamValidation::parseStatement));
    } catch (Exception e) {
      throw new IllegalStateException(
          "The dynamically constructed iam-policy for the "
              + bucketName
              + " bucket results in an invalid policy, check the client-iam.statements option",
          e);
    }
  }

  private static void parseStatement(String stmt) {
    requireJsonObjectShape(stmt);
    try (MappingIterator<JsonNode> values = MAPPER.readerFor(JsonNode.class).readValues(stmt)) {
      if (!values.hasNext()) {
        throw new IOException("Invalid statement");
      }
      JsonNode value = values.nextValue();
      if (!(value instanceof ObjectNode)) {
        throw new IOException("Invalid statement");
      }
      if (values.hasNext()) {
        throw new IOException("Invalid statement");
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static void requireJsonObjectShape(String stmt) {
    String trimmed = stmt.trim();
    if (!trimmed.startsWith("{") || !trimmed.endsWith("}")) {
      throw new RuntimeException(new IOException("Invalid statement"));
    }
  }
}
