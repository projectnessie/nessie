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

import static com.google.common.base.Preconditions.checkArgument;
import static org.projectnessie.catalog.files.config.S3IamValidation.validateIam;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.time.Duration;
import java.util.Optional;
import org.immutables.value.Value;

public interface S3Iam {
  /**
   * Default value for {@link #roleSessionName()} that identifies the session simply as a "Nessie"
   * session.
   */
  String DEFAULT_SESSION_NAME = "nessie";

  /** Default value for {@link #minSessionCredentialValidityPeriod()}. */
  Duration DEFAULT_SESSION_DURATION =
      Duration.ofHours(1); // 1 hour lifetime is common for session credentials in S3

  /**
   * Optional parameter to enable assume role (vended credentials). Default is to disable assume
   * role.
   */
  Optional<Boolean> enabled();

  /**
   * IAM policy in JSON format to be used as an inline <a
   * href="https://docs.aws.amazon.com/IAM/latest/UserGuide/access_policies.html#policies_session">session
   * policy</a> (optional).
   *
   * <p>If specified, this policy will be used for all clients for all locations.
   *
   * <p>Related docs: <a
   * href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/security_iam_service-with-iam.html">S3
   * with IAM</a> and <a
   * href="https://docs.aws.amazon.com/service-authorization/latest/reference/list_amazons3.html">about
   * actions, resources, conditions</a> and <a
   * href="https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies.html">policy
   * reference</a>.
   */
  Optional<String> policy();

  /**
   * The <a href="https://docs.aws.amazon.com/IAM/latest/UserGuide/reference-arns.html">ARN</a> of
   * the role to assume for accessing S3 data. This parameter is required for Amazon S3, but may not
   * be required for other storage providers (e.g. Minio does not use it at all).
   *
   * <p>If this option is defined, the server will attempt to assume the role at startup and cache
   * the returned session credentials.
   */
  Optional<String> assumeRole();

  /**
   * An identifier for the assumed role session. This parameter is most important in cases when the
   * same role is assumed by different principals in different use cases.
   */
  Optional<String> roleSessionName();

  /**
   * An identifier for the party assuming the role. This parameter must match the external ID
   * configured in IAM rules that <a
   * href="https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_create_for-user_externalid.html">govern</a>
   * the assume role process for the specified {@code role-arn}.
   *
   * <p>This parameter is essential in preventing the <a
   * href="https://docs.aws.amazon.com/IAM/latest/UserGuide/confused-deputy.html">Confused
   * Deputy</a> problem.
   */
  Optional<String> externalId();

  /**
   * A higher bound estimate of the expected duration of client "sessions" working with data in this
   * bucket. A session, for example, is the lifetime of an Iceberg REST catalog object on the client
   * side. This value is used for validating expiration times of credentials associated with the
   * warehouse. Must be >= 1 second.
   */
  Optional<Duration> sessionDuration();

  /**
   * The minimum required validity period for session credentials. The value of {@code
   * client-session-duration} is used if set, otherwise the default ({@code
   * DEFAULT_SESSION_DURATION}) session duration is assumed.
   */
  @Value.NonAttribute
  @JsonIgnore
  default Duration minSessionCredentialValidityPeriod() {
    return sessionDuration().orElse(DEFAULT_SESSION_DURATION);
  }

  default void validate(String bucketName) {
    validateIam(this, bucketName);
    sessionDuration()
        .ifPresent(
            duration -> {
              checkArgument(
                  !duration.isNegative(), "Requested session duration is negative: " + duration);
              long seconds = duration.toSeconds();
              checkArgument(seconds > 0, "Requested session duration is too short: " + duration);
              checkArgument(
                  seconds < Integer.MAX_VALUE,
                  "Requested session duration is too long: " + duration);
            });
  }
}
