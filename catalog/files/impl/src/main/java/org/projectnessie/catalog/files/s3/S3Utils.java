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

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import java.net.URI;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.projectnessie.catalog.files.config.S3AuthType;
import org.projectnessie.catalog.files.config.S3BucketOptions;
import org.projectnessie.catalog.secrets.BasicCredentials;
import org.projectnessie.catalog.secrets.SecretType;
import org.projectnessie.catalog.secrets.SecretsProvider;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

public final class S3Utils {

  private static final Pattern S3_HOST_PATTERN =
      Pattern.compile("^((.+)\\.)?s3[.-]([a-z0-9-]+)\\..*");
  // This matches any of
  //   /bucket
  //   /bucket/path
  // but not
  //   /bucket/
  private static final Pattern S3_PATH_PATTERN = Pattern.compile("^/([^/]+)(/.+|$)");

  private static final AwsCredentialsProvider DEFAULT_CREDENTIALS_PROVIDER =
      DefaultCredentialsProvider.builder().build();

  private S3Utils() {}

  public static Optional<String> extractBucketName(URI uri) {
    String scheme = requireNonNull(uri, "URI argument missing").getScheme();
    return switch (scheme) {
      case "s3", "s3a", "s3n" -> extractBucketFromS3Uri(uri);
      case "http", "https" -> extractBucketFromHttpUri(uri);
      default -> throw new IllegalArgumentException("Unsupported URI scheme: " + scheme);
    };
  }

  public static boolean isS3scheme(String scheme) {
    if (scheme == null) {
      return false;
    }
    return switch (scheme) {
      case "s3", "s3a", "s3n" -> true;
      default -> false;
    };
  }

  public static String normalizeS3Scheme(String uri) {
    if (uri.startsWith("s3a://")) {
      return uri.replaceFirst("s3a://", "s3://");
    }
    if (uri.startsWith("s3n://")) {
      return uri.replaceFirst("s3n://", "s3://");
    }
    return uri;
  }

  private static Optional<String> extractBucketFromHttpUri(URI uri) {
    String host = uri.getHost();
    checkArgument(host != null, "No host in non-s3 scheme URI: '%s'", uri);

    Matcher matcher = S3_HOST_PATTERN.matcher(host);
    if (matcher.matches()) {
      // A host with a URI using either "s3-region" or "s3.region" syntax
      String bucket = matcher.group(2);
      if (bucket != null) {
        return Optional.of(bucket);
      }
    }

    String path = uri.getPath();
    matcher = S3_PATH_PATTERN.matcher(path);
    return Optional.ofNullable(matcher.matches() ? matcher.group(1) : null);
  }

  private static Optional<String> extractBucketFromS3Uri(URI uri) {
    String auth = uri.getAuthority();
    return Optional.ofNullable(auth);
  }

  /**
   * Converts an S3 HTTP URI to an S3 location, recognizing virtual-hosted style only for hosts that
   * look like AWS S3 endpoints ({@code <bucket>.s3.<region>...} or {@code <bucket>.s3-<region>...})
   * and using path style otherwise.
   *
   * <p>This is only a heuristic. Use {@link #asS3VirtualHostedLocation(String, String)} or {@link
   * #asS3PathStyleLocation(String)} when the addressing style is known.
   */
  public static String asS3Location(String uri) {
    URI httpUri = parseHttpUri(uri);
    Matcher matcher = S3_HOST_PATTERN.matcher(httpUri.getHost());
    if (matcher.matches() && matcher.group(2) != null) {
      // virtual-hosted style
      return "s3://" + matcher.group(2) + httpUri.getPath();
    }
    return pathStyleLocation(httpUri);
  }

  /**
   * Converts an S3 HTTP URI for {@code bucket} to an S3 location, using {@linkplain
   * #asS3VirtualHostedLocation(String, String) virtual-hosted style} if the host starts with {@code
   * bucket}, {@linkplain #asS3PathStyleLocation(String) path style} otherwise.
   */
  public static String asS3Location(String uri, String bucket) {
    return asS3VirtualHostedLocation(uri, bucket).orElseGet(() -> asS3PathStyleLocation(uri));
  }

  /**
   * Converts a path-style S3 HTTP URI, as in {@code https://<endpoint>/<bucket>/<key>}, to an S3
   * location. The host is not considered.
   */
  public static String asS3PathStyleLocation(String uri) {
    return pathStyleLocation(parseHttpUri(uri));
  }

  /**
   * Converts a virtual-hosted-style S3 HTTP URI for {@code bucket}, as in {@code
   * https://<bucket>.<endpoint>/<key>}, to an S3 location.
   *
   * <p>This only checks that {@code bucket} is the leading part of the host. Bucket names can
   * contain dots, so a host for bucket {@code foo.bar} also starts with {@code foo.}; callers that
   * need to rule that out must check the remainder of the host against the endpoint.
   *
   * @return the S3 location, or empty if the host does not start with {@code bucket}
   */
  public static Optional<String> asS3VirtualHostedLocation(String uri, String bucket) {
    requireNonNull(bucket, "Bucket missing");
    URI httpUri = parseHttpUri(uri);
    if (!httpUri.getHost().startsWith(bucket + ".")) {
      return Optional.empty();
    }
    return Optional.of("s3://" + bucket + httpUri.getPath());
  }

  private static String pathStyleLocation(URI httpUri) {
    String httpUriPath = httpUri.getPath();
    Matcher matcher = S3_PATH_PATTERN.matcher(httpUriPath);
    checkArgument(matcher.matches(), "Invalid S3 URI: '%s'", httpUri);
    String bucket = matcher.group(1);
    return "s3://" + bucket + httpUriPath.substring(bucket.length() + 1);
  }

  private static URI parseHttpUri(String uri) {
    URI httpUri = URI.create(uri);
    checkArgument(httpUri.getScheme() != null, "No scheme in URI: '%s'", httpUri);
    checkArgument(httpUri.getHost() != null, "No host in URI: '%s'", httpUri);
    checkArgument(
        httpUri.getScheme().matches("http|https"),
        "Unsupported URI scheme: '%s'",
        httpUri.getScheme());
    return httpUri;
  }

  public static String iamEscapeString(String s) {
    // See
    // https://steampipe.io/blog/aws-iam-policy-wildcards-reference#bonus-how-to-escape-special-characters
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      switch (c) {
        case '*', '?', '$' -> sb.append("${").append(c).append('}');
        default -> sb.append(c);
      }
    }
    return sb.toString();
  }

  public static AwsCredentialsProvider newCredentialsProvider(
      S3AuthType authType, S3BucketOptions bucketOptions, SecretsProvider secretsProvider) {
    return switch (authType) {
      case APPLICATION_GLOBAL -> DEFAULT_CREDENTIALS_PROVIDER;
      case STATIC -> {
        URI secretName =
            bucketOptions
                .accessKey()
                .orElseThrow(
                    () ->
                        new IllegalArgumentException(
                            "Missing access key and secret for STATIC authentication mode"));

        yield secretsProvider
            .getSecret(secretName, SecretType.BASIC, BasicCredentials.class)
            .map(key -> AwsBasicCredentials.create(key.name(), key.secret()))
            .map(creds -> (AwsCredentialsProvider) StaticCredentialsProvider.create(creds))
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        "Missing access key and secret for STATIC authentication mode"));
      }
      default -> throw new IllegalArgumentException("Unsupported S3 auth type: " + authType);
    };
  }
}
