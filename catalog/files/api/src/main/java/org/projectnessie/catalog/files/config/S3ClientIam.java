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

import static org.projectnessie.catalog.files.config.S3IamValidation.validateClientIam;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.List;
import java.util.Optional;
import org.immutables.value.Value;
import org.projectnessie.nessie.immutables.NessieImmutable;

@SuppressWarnings(
    "DefaultAnnotationParam") // MUST specify 'all Parameters = false' for some reason :shrug_
@NessieImmutable
@Value.Style(allParameters = false)
@JsonSerialize(as = ImmutableS3ClientIam.class)
@JsonDeserialize(as = ImmutableS3ClientIam.class)
public interface S3ClientIam extends S3Iam {

  /**
   * Additional IAM policy statements to be inserted <em>after</em> the automatically generated S3
   * location dependent {@code Allow} policy statement.
   *
   * <p>Example:
   *
   * <p><code>
   * ...client-iam.statements[0]={"Effect":"Allow", "Action":"s3:*", "Resource":"arn:aws:s3:::*&#47;alwaysAllowed&#47;*"}
   * ...client-iam.statements[1]={"Effect":"Deny", "Action":"s3:*", "Resource":"arn:aws:s3:::*&#47;blocked&#47;*"}
   * </code>
   *
   * <p>Related docs: <a
   * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/security_iam_service-with-iam.html">S3
   * with IAM</a> and <a
   * href="https://docs.aws.amazon.com/service-authorization/latest/reference/list_amazons3.html">about
   * actions, resources, conditions</a> and <a
   * href="https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies.html">policy
   * reference</a>.
   */
  Optional<List<String>> statements();

  @Override
  default void validate(String bucketName) {
    validateClientIam(this, bucketName);
  }
}
