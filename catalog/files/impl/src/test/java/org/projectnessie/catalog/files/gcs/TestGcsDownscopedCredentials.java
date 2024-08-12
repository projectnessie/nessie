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
package org.projectnessie.catalog.files.gcs;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.projectnessie.catalog.files.gcs.GcsStorageSupplier.RANDOMIZED_PART;

import com.google.auth.oauth2.CredentialAccessBoundary;
import com.google.auth.oauth2.CredentialAccessBoundary.AccessBoundaryRule;
import com.google.auth.oauth2.CredentialAccessBoundary.AccessBoundaryRule.AvailabilityCondition;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.catalog.files.api.StorageLocations;
import org.projectnessie.cel.tools.Script;
import org.projectnessie.storage.uri.StorageUri;

/**
 * Contains some "best effort" test cases to validate that the {@link CredentialAccessBoundary}
 * objects and the CEL expressions used for access checks are correct.
 */
@ExtendWith(SoftAssertionsExtension.class)
public class TestGcsDownscopedCredentials {
  @InjectSoftAssertions protected SoftAssertions soft;

  /**
   * Check whether the {@code CredentialAccessBoundary} objects match the expectation for a set of
   * read-only and writeable locations..
   */
  @ParameterizedTest
  @MethodSource
  public void generateAccessBoundaryRules(
      StorageLocations storageLocations, CredentialAccessBoundary expected) {
    CredentialAccessBoundary actual =
        GcsStorageSupplier.generateAccessBoundaryRules(storageLocations);
    assertThat(
            actual.getAccessBoundaryRules().stream()
                .map(
                    rule ->
                        tuple(
                            rule.getAvailabilityCondition().getExpression(),
                            rule.getAvailableResource(),
                            rule.getAvailablePermissions()))
                .collect(Collectors.toList()))
        .containsExactlyInAnyOrderElementsOf(
            expected.getAccessBoundaryRules().stream()
                .map(
                    rule ->
                        tuple(
                            rule.getAvailabilityCondition().getExpression(),
                            rule.getAvailableResource(),
                            rule.getAvailablePermissions()))
                .collect(Collectors.toList()));
  }

  /**
   * Validate the CEL syntax in generated {@link CredentialAccessBoundary} object and whether those
   * CEL expressions execute.
   */
  @ParameterizedTest
  @MethodSource("generateAccessBoundaryRules")
  public void validateCelSyntax(
      StorageLocations storageLocations,
      @SuppressWarnings("unused") CredentialAccessBoundary expected) {
    CredentialAccessBoundary actual =
        GcsStorageSupplier.generateAccessBoundaryRules(storageLocations);

    CelEval.ResourceComposite resource = new CelEval.ResourceComposite();
    resource.setName("gs://bucket/blah");

    for (AccessBoundaryRule rule : actual.getAccessBoundaryRules()) {
      String expr = rule.getAvailabilityCondition().getExpression();

      soft.assertThatCode(
              () -> {
                Script script = CelEval.scriptFor(expr);

                assertThat(
                        script.execute(
                            Boolean.class,
                            Map.of(
                                "api", new CelEval.ApiInterface(Map.of()), "resource", resource)))
                    .isNotNull();
              })
          .describedAs("Script '%s'", expr)
          .doesNotThrowAnyException();
    }
  }

  /**
   * Verify whether CEL expressions for a storage location given passes as expected.
   *
   * @param resource the storage location used to generate the CEL expression, also base of the
   *     "positive checks"
   * @param negativeCheck a storage location that must not pass ("negative check") against the
   *     generated CEL expression.
   */
  @ParameterizedTest
  @MethodSource
  public void resourceNameBucketPathExpression(String resource, String negativeCheck)
      throws Exception {

    String bucketPositive = StorageUri.of(resource).requiredAuthority();
    String pathPositive = StorageUri.of(resource).pathWithoutLeadingTrailingSlash();

    String bucketNegative = StorageUri.of(negativeCheck).requiredAuthority();
    String pathNegative = StorageUri.of(negativeCheck).pathWithoutLeadingTrailingSlash();

    for (String res :
        new String[] {resource, resource + "/hello.txt", resource + "/much/deeper/hello.txt"}) {

      String bucket = StorageUri.of(res).requiredAuthority();
      String path = StorageUri.of(res).pathWithoutLeadingTrailingSlash();

      CelEval.ResourceComposite r = new CelEval.ResourceComposite();

      String expr =
          GcsStorageSupplier.resourceNameBucketPathExpression(bucketPositive, pathPositive);
      Script script = CelEval.scriptFor(expr);

      r.setName(format("projects/_/buckets/%s/objects/%s", bucket, path));
      soft.assertThat(
              script.execute(
                  Boolean.class, Map.of("api", new CelEval.ApiInterface(Map.of()), "resource", r)))
          .describedAs("positive, resource: %s", res)
          .isEqualTo(true);

      r.setName(format("projects/_/buckets/%s/objects/deadbeef/%s", bucket, path));
      soft.assertThat(
              script.execute(
                  Boolean.class, Map.of("api", new CelEval.ApiInterface(Map.of()), "resource", r)))
          .describedAs("positive, object, resource: %s", res)
          .isEqualTo(true);

      // negative checks

      expr = GcsStorageSupplier.resourceNameBucketPathExpression(bucketNegative, pathNegative);
      script = CelEval.scriptFor(expr);

      r.setName(format("projects/_/buckets/%s/objects/%s", bucket, path));
      soft.assertThat(
              script.execute(
                  Boolean.class, Map.of("api", new CelEval.ApiInterface(Map.of()), "resource", r)))
          .describedAs("negative, resource: %s", res)
          .isEqualTo(false);

      r.setName(format("projects/_/buckets/%s/objects/deadbeef/%s", bucket, path));
      soft.assertThat(
              script.execute(
                  Boolean.class, Map.of("api", new CelEval.ApiInterface(Map.of()), "resource", r)))
          .describedAs("negative, object, resource: %s", res)
          .isEqualTo(false);
    }
  }

  static Stream<Arguments> resourceNameBucketPathExpression() {
    return Stream.of(
        arguments("gs://bucket/foo/bar/baz", "gs://bucket/foo/bar/foo"),
        //
        arguments("gs://bucket/foo/b'ar/baz", "gs://bucket/foo/b.ar/baz"),
        //
        arguments("gs://bucket/foo/bar/b\"az", "gs://buc,et/foo/bar/b\"az"),
        //
        arguments("gs://buck.et/foo/bar/b.az", "gs://buckxet/foo/bar/bxaz"),
        //
        arguments("gs://bucket/foo/bar/b.az", "gs://buckxet/foo/bar/bxaz")
        //
        );
  }

  static Stream<Arguments> generateAccessBoundaryRules() {
    String bucketName = "my-bucket-name";
    String bucket2 = "bucket2";
    String bucket3 = "bucket3";
    String readOnly = "read-only";
    String read2 = "read2";
    String read3 = "read3";
    String writeable = "write-able";
    String write2 = "write2";
    String write3 = "write3";
    return Stream.of(
        //
        // [1] one writeable location
        arguments(
            StorageLocations.storageLocations(
                StorageUri.of("s3://not-used/"),
                List.of(StorageUri.of("gs://" + bucketName + "/" + writeable)),
                List.of()),
            CredentialAccessBoundary.newBuilder()
                .addRule(
                    AccessBoundaryRule.newBuilder()
                        .setAvailabilityCondition(
                            AvailabilityCondition.newBuilder()
                                .setExpression(
                                    resourceNameMatches(bucketName, writeable)
                                        + " || "
                                        + listPrefix(writeable))
                                .build())
                        .setAvailableResource(
                            "//storage.googleapis.com/projects/_/buckets/" + bucketName)
                        .addAvailablePermission("inRole:roles/storage.legacyObjectReader")
                        .addAvailablePermission("inRole:roles/storage.objectViewer")
                        .build())
                .addRule(
                    AccessBoundaryRule.newBuilder()
                        .setAvailabilityCondition(
                            AvailabilityCondition.newBuilder()
                                .setExpression(resourceNameMatches(bucketName, writeable))
                                .build())
                        .setAvailableResource(
                            "//storage.googleapis.com/projects/_/buckets/" + bucketName)
                        .addAvailablePermission("inRole:roles/storage.legacyBucketWriter")
                        .build())
                .build()),
        //
        // [2] one read-only
        arguments(
            StorageLocations.storageLocations(
                StorageUri.of("s3://not-used/"),
                List.of(),
                List.of(StorageUri.of("gs://" + bucketName + "/" + readOnly))),
            CredentialAccessBoundary.newBuilder()
                .addRule(
                    AccessBoundaryRule.newBuilder()
                        .setAvailabilityCondition(
                            AvailabilityCondition.newBuilder()
                                .setExpression(
                                    resourceNameMatches(bucketName, readOnly)
                                        + " || "
                                        + listPrefix(readOnly))
                                .build())
                        .setAvailableResource(
                            "//storage.googleapis.com/projects/_/buckets/" + bucketName)
                        .addAvailablePermission("inRole:roles/storage.legacyObjectReader")
                        .addAvailablePermission("inRole:roles/storage.objectViewer")
                        .build())
                .build()),
        //
        // [3] one writeable location + one read-only
        arguments(
            StorageLocations.storageLocations(
                StorageUri.of("s3://not-used/"),
                List.of(StorageUri.of("gs://" + bucketName + "/" + writeable)),
                List.of(StorageUri.of("gs://" + bucketName + "/" + readOnly))),
            CredentialAccessBoundary.newBuilder()
                .addRule(
                    AccessBoundaryRule.newBuilder()
                        .setAvailabilityCondition(
                            AvailabilityCondition.newBuilder()
                                .setExpression(
                                    resourceNameMatches(bucketName, readOnly)
                                        + " || "
                                        + listPrefix(readOnly)
                                        + " || "
                                        + resourceNameMatches(bucketName, writeable)
                                        + " || "
                                        + listPrefix(writeable))
                                .build())
                        .setAvailableResource(
                            "//storage.googleapis.com/projects/_/buckets/" + bucketName)
                        .addAvailablePermission("inRole:roles/storage.legacyObjectReader")
                        .addAvailablePermission("inRole:roles/storage.objectViewer")
                        .build())
                .addRule(
                    AccessBoundaryRule.newBuilder()
                        .setAvailabilityCondition(
                            AvailabilityCondition.newBuilder()
                                .setExpression(resourceNameMatches(bucketName, writeable))
                                .build())
                        .setAvailableResource(
                            "//storage.googleapis.com/projects/_/buckets/" + bucketName)
                        .addAvailablePermission("inRole:roles/storage.legacyBucketWriter")
                        .build())
                .build()),
        //
        // [4] two writeable locations + two read-only locations
        arguments(
            StorageLocations.storageLocations(
                StorageUri.of("s3://not-used/"),
                List.of(
                    StorageUri.of("gs://" + bucketName + "/" + writeable),
                    StorageUri.of("gs://" + bucketName + "/" + write2)),
                List.of(
                    StorageUri.of("gs://" + bucketName + "/" + readOnly),
                    StorageUri.of("gs://" + bucketName + "/" + read2))),
            CredentialAccessBoundary.newBuilder()
                .addRule(
                    AccessBoundaryRule.newBuilder()
                        .setAvailabilityCondition(
                            AvailabilityCondition.newBuilder()
                                .setExpression(
                                    resourceNameMatches(bucketName, readOnly)
                                        + " || "
                                        + listPrefix(readOnly)
                                        + " || "
                                        + resourceNameMatches(bucketName, read2)
                                        + " || "
                                        + listPrefix(read2)
                                        + " || "
                                        + resourceNameMatches(bucketName, writeable)
                                        + " || "
                                        + listPrefix(writeable)
                                        + " || "
                                        + resourceNameMatches(bucketName, write2)
                                        + " || "
                                        + listPrefix(write2))
                                .build())
                        .setAvailableResource(
                            "//storage.googleapis.com/projects/_/buckets/" + bucketName)
                        .addAvailablePermission("inRole:roles/storage.legacyObjectReader")
                        .addAvailablePermission("inRole:roles/storage.objectViewer")
                        .build())
                .addRule(
                    AccessBoundaryRule.newBuilder()
                        .setAvailabilityCondition(
                            AvailabilityCondition.newBuilder()
                                .setExpression(
                                    resourceNameMatches(bucketName, writeable)
                                        + " || "
                                        + resourceNameMatches(bucketName, write2))
                                .build())
                        .setAvailableResource(
                            "//storage.googleapis.com/projects/_/buckets/" + bucketName)
                        .addAvailablePermission("inRole:roles/storage.legacyBucketWriter")
                        .build())
                .build()),
        //
        // [5] writeable locations across buckets + read-only locations across buckets
        arguments(
            StorageLocations.storageLocations(
                StorageUri.of("s3://not-used/"),
                List.of(
                    StorageUri.of("gs://" + bucketName + "/" + writeable),
                    StorageUri.of("gs://" + bucket2 + "/" + write2),
                    StorageUri.of("gs://" + bucket3 + "/" + write3)),
                List.of(
                    StorageUri.of("gs://" + bucketName + "/" + readOnly),
                    StorageUri.of("gs://" + bucket2 + "/" + read2),
                    StorageUri.of("gs://" + bucket3 + "/" + read3))),
            CredentialAccessBoundary.newBuilder()
                .addRule(
                    AccessBoundaryRule.newBuilder()
                        .setAvailabilityCondition(
                            AvailabilityCondition.newBuilder()
                                .setExpression(
                                    resourceNameMatches(bucketName, readOnly)
                                        + " || "
                                        + listPrefix(readOnly)
                                        + " || "
                                        + resourceNameMatches(bucketName, writeable)
                                        + " || "
                                        + listPrefix(writeable))
                                .build())
                        .setAvailableResource(
                            "//storage.googleapis.com/projects/_/buckets/" + bucketName)
                        .addAvailablePermission("inRole:roles/storage.legacyObjectReader")
                        .addAvailablePermission("inRole:roles/storage.objectViewer")
                        .build())
                .addRule(
                    AccessBoundaryRule.newBuilder()
                        .setAvailabilityCondition(
                            AvailabilityCondition.newBuilder()
                                .setExpression(
                                    resourceNameMatches(bucket2, read2)
                                        + " || "
                                        + listPrefix(read2)
                                        + " || "
                                        + resourceNameMatches(bucket2, write2)
                                        + " || "
                                        + listPrefix(write2))
                                .build())
                        .setAvailableResource(
                            "//storage.googleapis.com/projects/_/buckets/" + bucket2)
                        .addAvailablePermission("inRole:roles/storage.legacyObjectReader")
                        .addAvailablePermission("inRole:roles/storage.objectViewer")
                        .build())
                .addRule(
                    AccessBoundaryRule.newBuilder()
                        .setAvailabilityCondition(
                            AvailabilityCondition.newBuilder()
                                .setExpression(
                                    resourceNameMatches(bucket3, read3)
                                        + " || "
                                        + listPrefix(read3)
                                        + " || "
                                        + resourceNameMatches(bucket3, write3)
                                        + " || "
                                        + listPrefix(write3))
                                .build())
                        .setAvailableResource(
                            "//storage.googleapis.com/projects/_/buckets/" + bucket3)
                        .addAvailablePermission("inRole:roles/storage.legacyObjectReader")
                        .addAvailablePermission("inRole:roles/storage.objectViewer")
                        .build())
                .addRule(
                    AccessBoundaryRule.newBuilder()
                        .setAvailabilityCondition(
                            AvailabilityCondition.newBuilder()
                                .setExpression(resourceNameMatches(bucketName, writeable))
                                .build())
                        .setAvailableResource(
                            "//storage.googleapis.com/projects/_/buckets/" + bucketName)
                        .addAvailablePermission("inRole:roles/storage.legacyBucketWriter")
                        .build())
                .addRule(
                    AccessBoundaryRule.newBuilder()
                        .setAvailabilityCondition(
                            AvailabilityCondition.newBuilder()
                                .setExpression(resourceNameMatches(bucket2, write2))
                                .build())
                        .setAvailableResource(
                            "//storage.googleapis.com/projects/_/buckets/" + bucket2)
                        .addAvailablePermission("inRole:roles/storage.legacyBucketWriter")
                        .build())
                .addRule(
                    AccessBoundaryRule.newBuilder()
                        .setAvailabilityCondition(
                            AvailabilityCondition.newBuilder()
                                .setExpression(resourceNameMatches(bucket3, write3))
                                .build())
                        .setAvailableResource(
                            "//storage.googleapis.com/projects/_/buckets/" + bucket3)
                        .addAvailablePermission("inRole:roles/storage.legacyBucketWriter")
                        .build())
                .build())
        //
        );
  }

  private static String listPrefix(String readOnly) {
    return "api.getAttribute('storage.googleapis.com/objectListPrefix', '').matches('"
        + RANDOMIZED_PART
        + "\\\\Q"
        + readOnly
        + "\\\\E.*')";
  }

  private static String resourceNameMatches(String bucketName, String writeable) {
    return "resource.name.matches('\\\\Qprojects/_/buckets/"
        + bucketName
        + "/objects/\\\\E"
        + RANDOMIZED_PART
        + "\\\\Q"
        + writeable
        + "\\\\E.*')";
  }
}
