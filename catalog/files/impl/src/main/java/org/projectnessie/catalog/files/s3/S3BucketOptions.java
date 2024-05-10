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

import java.net.URI;
import java.time.Duration;
import java.util.Optional;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;

public interface S3BucketOptions {
  /**
   * Default value for {@link #roleSessionName()} that identifies the session simply as a "Nessie"
   * session.
   */
  String DEFAULT_SESSION_NAME = "nessie";

  /** Default value for {@link #minSessionCredentialValidityPeriod()}. */
  Duration DEFAULT_SESSION_DURATION =
      Duration.ofHours(1); // 1 hour lifetime is common for session credentials in S3

  /**
   * Default value for {@link #clientAuthenticationMode()}, being {@link
   * S3ClientAuthenticationMode#REQUEST_SIGNING}.
   */
  S3ClientAuthenticationMode DEFAULT_AUTHENTICATION_MODE =
      S3ClientAuthenticationMode.REQUEST_SIGNING;

  /**
   * The type of cloud running the S3 service. The cloud type must be configured, either per bucket
   * or in {@link S3Options S3Options}.
   */
  Optional<Cloud> cloud();

  /**
   * Endpoint URI, required for {@linkplain Cloud#PRIVATE private clouds}. The endpoint must be
   * specified for {@linkplain Cloud#PRIVATE private clouds}, either per bucket or in {@link
   * S3Options S3Options}.
   *
   * <p>If the endpoint URIs for the Nessie server and clients differ, this one defines the endpoint
   * used for the Nessie server.
   */
  Optional<URI> endpoint();

  /**
   * When using a {@linkplain #endpoint() specific endpoint} and the endpoint URIs for the Nessie
   * server differ, you can specify the URI passed down to clients using this setting. Otherwise
   * clients will receive the value from the {@link #endpoint()} setting.
   */
  Optional<URI> externalEndpoint();

  /**
   * Whether to use path-style access. If true, path-style access will be used, as in: {@code
   * https://<domain>/<bucket>}. If false, a virtual-hosted style will be used instead, as in:
   * {@code https://<bucket>.<domain>}. If unspecified, the default will depend on the cloud
   * provider.
   */
  Optional<Boolean> pathStyleAccess();

  /**
   * AWS Access point for this bucket. Access points can be used to perform S3 operations by
   * specifying a mapping of bucket to access points. This is useful for multi-region access,
   * cross-region access, disaster recovery, etc.
   *
   * @see <a
   *     href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-access-points.html">Access
   *     Points</a>
   */
  Optional<String> accessPoint();

  /**
   * Authorize cross-region calls when contacting an {@link #accessPoint()}.
   *
   * <p>By default, attempting to use an access point in a different region will throw an exception.
   * When enabled, this property allows using access points in other regions.
   */
  Optional<Boolean> allowCrossRegionAccessPoint();

  /**
   * DNS name of the region, required for {@linkplain Cloud#AMAZON AWS}. The region must be
   * specified for {@linkplain Cloud#AMAZON AWS}, either per bucket or in {@link S3Options
   * S3Options}.
   */
  Optional<String> region();

  /** Project ID, if required, for example for {@linkplain Cloud#GOOGLE Google cloud}. */
  Optional<String> projectId();

  /**
   * Key used to look up the access-key-ID via {@link
   * org.projectnessie.catalog.files.secrets.SecretsProvider SecretsProvider}. An access-key-id must
   * be configured, either per bucket or in {@link S3Options S3Options}. For STS, this defines the
   * Access Key ID to be used as a basic credential for obtaining temporary session credentials.
   */
  Optional<String> accessKeyIdRef();

  /**
   * Key used to look up the secret-access-key via {@link
   * org.projectnessie.catalog.files.secrets.SecretsProvider SecretsProvider}. A secret-access-key
   * must be configured, either per bucket or in {@link S3Options S3Options}. For STS, this defines
   * the Secret Key ID to be used as a basic credential for obtaining temporary session credentials.
   */
  Optional<String> secretAccessKeyRef();

  /**
   * The <a href="https://docs.aws.amazon.com/STS/latest/APIReference/welcome.html">Security Token
   * Service</a> endpoint.
   *
   * <p>This parameter must be set if the cloud provider is not {@link Cloud#AMAZON}) and the
   * catalog is configured to use S3 sessions (e.g. to use the "assume role" functionality).
   */
  Optional<URI> stsEndpoint();

  /**
   * The <a href="https://docs.aws.amazon.com/IAM/latest/UserGuide/reference-arns.html">ARN</a> of
   * the role to assume for accessing S3 data. This parameter is required for Amazon S3, but may not
   * be required for other storage providers (e.g. Minio does not use it at all).
   */
  Optional<String> roleArn();

  /**
   * IAM policy in JSON format to be used as an inline <a
   * href="https://docs.aws.amazon.com/IAM/latest/UserGuide/access_policies.html#policies_session">session
   * policy</a> (optional).
   *
   * @see AssumeRoleRequest#policy()
   */
  Optional<String> iamPolicy();

  /**
   * An identifier for the assumed role session. This parameter is most important in cases when the
   * same role is assumed by different principals in different use cases.
   *
   * @see AssumeRoleRequest#roleSessionName()
   */
  Optional<String> roleSessionName();

  /**
   * An identifier for the party assuming the role. This parameter must match the external ID
   * configured in IAM rules that <a
   * href="https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_create_for-user_externalid.html">govern</a>
   * the assume role process for the specified {@link #roleArn()}.
   *
   * <p>This parameter is essential in preventing the <a
   * href="https://docs.aws.amazon.com/IAM/latest/UserGuide/confused-deputy.html">Confused
   * Deputy</a> problem.
   *
   * @see AssumeRoleRequest#externalId()
   */
  Optional<String> externalId();

  /** Controls the authentication mode for Catalog clients accessing this bucket. */
  Optional<S3ClientAuthenticationMode> clientAuthenticationMode();

  default S3ClientAuthenticationMode effectiveClientAuthenticationMode() {
    return clientAuthenticationMode().orElse(DEFAULT_AUTHENTICATION_MODE);
  }

  /**
   * A higher bound estimate of the expected duration of client "sessions" working with data in this
   * bucket. A session, for example, is the lifetime of an Iceberg REST catalog object on the client
   * side. This value is used for validating expiration times of credentials associated with the
   * warehouse.
   *
   * <p>This parameter is relevant only when {@link #clientAuthenticationMode()} is {@link
   * S3ClientAuthenticationMode#ASSUME_ROLE}.
   */
  Optional<Duration> clientSessionDuration();

  /**
   * The minimum required validity period for session credentials. The value of {@link
   * #clientSessionDuration()} is used if set, otherwise the {@link #DEFAULT_SESSION_DURATION
   * default} session duration is assumed.
   */
  default Duration minSessionCredentialValidityPeriod() {
    return clientSessionDuration().orElse(DEFAULT_SESSION_DURATION);
  }

  /**
   * Extract the bucket name from the URI. Only relevant for {@linkplain Cloud#AMAZON AWS} or
   * {@linkplain Cloud#PRIVATE S3-compatible private} clouds; the behavior of this method is
   * unspecified for other clouds.
   *
   * @param uri URI to extract the bucket name from; both s3 and https schemes are accepted, and
   *     https schemes can be either path-style or virtual-host-style.
   */
  default Optional<String> extractBucket(URI uri) {
    return S3Utils.extractBucketName(uri);
  }
}
