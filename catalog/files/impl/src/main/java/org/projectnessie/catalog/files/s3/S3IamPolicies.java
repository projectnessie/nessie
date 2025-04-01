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

import static java.lang.String.format;
import static org.projectnessie.catalog.files.s3.S3Utils.iamEscapeString;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.Scheduler;
import java.io.IOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;
import org.projectnessie.catalog.files.api.StorageLocations;
import org.projectnessie.catalog.files.config.S3ClientIam;
import org.projectnessie.storage.uri.StorageUri;

final class S3IamPolicies {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private S3IamPolicies() {}

  static String locationDependentPolicy(S3ClientIam clientIam, StorageLocations locations) {

    // See https://docs.aws.amazon.com/AmazonS3/latest/userguide/security_iam_service-with-iam.html
    // See https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies.html
    // See https://docs.aws.amazon.com/service-authorization/latest/reference/list_amazons3.html

    // Add necessary "Allow" statements for the given location. Use Jackson's mechanics here to
    // benefit from its proper JSON value escaping.

    ObjectNode policy = JsonNodeFactory.instance.objectNode();
    policy.put("Version", "2012-10-17");

    ArrayNode statements = policy.withArray("Statement");

    // "Allow" for the special 's3:listBucket' case, must be applied on the bucket, not the path.
    for (Iterator<StorageUri> locationIter =
            Stream.concat(
                    locations.writeableLocations().stream(), locations.readonlyLocations().stream())
                .iterator();
        locationIter.hasNext(); ) {
      StorageUri location = locationIter.next();
      String bucket = iamEscapeString(location.requiredAuthority());
      String path = iamEscapeString(location.pathWithoutLeadingTrailingSlash());

      ObjectNode statement = statements.addObject();
      statement.put("Effect", "Allow");
      statement.put("Action", "s3:ListBucket");
      statement.put("Resource", format("arn:aws:s3:::%s", bucket));
      ObjectNode condition = statement.withObject("Condition");
      ObjectNode stringLike = condition.withObject("StringLike");
      ArrayNode s3Prefix = stringLike.withArray("s3:prefix");
      s3Prefix.add(path);
      s3Prefix.add(path + "/*");
      // For write.object-storage.enabled=true / before Iceberg 1.7.0
      s3Prefix.add("*/" + path);
      s3Prefix.add("*/" + path + "/*");
      // For write.object-storage.enabled=true / after Iceberg 1.7.0
      s3Prefix.add("*/*/*/*/" + path);
      s3Prefix.add("*/*/*/*/" + path + "/*");
    }

    // "Allow Write" for all remaining S3 actions on the bucket+path.
    List<StorageUri> writeable = locations.writeableLocations();
    if (!writeable.isEmpty()) {
      ObjectNode statement = statements.addObject();
      statement.put("Effect", "Allow");
      ArrayNode actions = statement.putArray("Action");
      actions.add("s3:GetObject");
      actions.add("s3:GetObjectVersion");
      actions.add("s3:PutObject");
      actions.add("s3:DeleteObject");
      ArrayNode resources = statement.withArray("Resource");
      for (StorageUri location : writeable) {
        String bucket = iamEscapeString(location.requiredAuthority());
        String path = iamEscapeString(location.pathWithoutLeadingTrailingSlash());
        resources.add(format("arn:aws:s3:::%s/%s/*", bucket, path));
        // For write.object-storage.enabled=true / before Iceberg 1.7.0
        resources.add(format("arn:aws:s3:::%s/*/%s/*", bucket, path));
        // For write.object-storage.enabled=true / since Iceberg 1.7.0
        resources.add(format("arn:aws:s3:::%s/*/*/*/*/%s/*", bucket, path));
      }
    }

    // "Allow read" for all remaining S3 actions on the bucket+path.
    List<StorageUri> readonly = locations.readonlyLocations();
    if (!readonly.isEmpty()) {
      ObjectNode statement = statements.addObject();
      statement.put("Effect", "Allow");
      ArrayNode actions = statement.putArray("Action");
      actions.add("s3:GetObject");
      actions.add("s3:GetObjectVersion");
      ArrayNode resources = statement.withArray("Resource");
      for (StorageUri location : readonly) {
        String bucket = iamEscapeString(location.requiredAuthority());
        String path = iamEscapeString(location.pathWithoutLeadingTrailingSlash());
        resources.add(format("arn:aws:s3:::%s%s/*", bucket, path));
        // For write.object-storage.enabled=true / before Iceberg 1.7.0
        resources.add(format("arn:aws:s3:::%s/*/%s/*", bucket, path));
        // For write.object-storage.enabled=true / since Iceberg 1.7.0
        resources.add(format("arn:aws:s3:::%s/*/*/*/*/%s/*", bucket, path));
      }
    }

    // Add custom statements
    clientIam
        .statements()
        .ifPresent(
            stmts ->
                stmts.stream().map(ParsedIamStatements.STATEMENTS::get).forEach(statements::add));

    return policy.toString();
  }

  private static class ParsedIamStatements {
    static final LoadingCache<String, ObjectNode> STATEMENTS =
        Caffeine.newBuilder()
            .maximumSize(2000)
            .expireAfterAccess(Duration.of(1, ChronoUnit.HOURS))
            .scheduler(Scheduler.systemScheduler())
            .build(ParsedIamStatements::parseStatement);

    private static ObjectNode parseStatement(String stmt) {
      try (MappingIterator<Object> values = MAPPER.readerFor(ObjectNode.class).readValues(stmt)) {
        ObjectNode node = null;
        if (values.hasNext()) {
          Object value = values.nextValue();
          if (value instanceof ObjectNode) {
            node = (ObjectNode) value;
          } else {
            throw new IOException("Invalid statement");
          }
        }
        if (values.hasNext()) {
          throw new IOException("Invalid statement");
        }
        return node;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
