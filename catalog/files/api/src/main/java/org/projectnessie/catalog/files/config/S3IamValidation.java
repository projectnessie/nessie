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

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import software.amazon.awssdk.policybuilder.iam.IamPolicyReader;

final class S3IamValidation {
  private static final ObjectMapper MAPPER = new ObjectMapper();
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
    try (MappingIterator<Object> values = MAPPER.readerFor(ObjectNode.class).readValues(stmt)) {
      if (values.hasNext()) {
        Object value = values.nextValue();
        if (!(value instanceof ObjectNode)) {
          throw new IOException("Invalid statement");
        }
      }
      if (values.hasNext()) {
        throw new IOException("Invalid statement");
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
