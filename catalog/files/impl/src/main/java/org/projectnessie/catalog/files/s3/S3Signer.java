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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.util.Optional;
import org.projectnessie.catalog.files.api.ImmutableSigningResponse;
import org.projectnessie.catalog.files.api.RequestSigner;
import org.projectnessie.catalog.files.api.SigningRequest;
import org.projectnessie.catalog.files.api.SigningResponse;
import org.projectnessie.catalog.files.secrets.SecretsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.signer.AwsS3V4Signer;
import software.amazon.awssdk.auth.signer.internal.SignerConstant;
import software.amazon.awssdk.auth.signer.params.AwsS3V4SignerParams;
import software.amazon.awssdk.auth.signer.params.SignerChecksumParams;
import software.amazon.awssdk.core.checksums.Algorithm;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.regions.Region;

public class S3Signer implements RequestSigner {

  private final AwsS3V4Signer signer = AwsS3V4Signer.create();
  private final S3Options<? extends S3BucketOptions> s3Options;
  private final SecretsProvider secretsProvider;
  private final S3Sessions s3sessions;

  static final String EMPTY_BODY_SHA256 =
      "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

  public S3Signer(
      S3Options<? extends S3BucketOptions> s3Options,
      SecretsProvider secretsProvider,
      S3Sessions s3sessions) {
    this.s3Options = s3Options;
    this.secretsProvider = secretsProvider;
    this.s3sessions = s3sessions;
  }

  @Override
  public SigningResponse sign(String ref, String key, SigningRequest clientRequest) {

    URI uri = clientRequest.uri();
    Optional<String> body = clientRequest.body();

    if (body.isEmpty()
        && "post".equalsIgnoreCase(clientRequest.method())
        && uri.getQuery().contains("delete")) {
      throw new IllegalArgumentException("DELETE requests must have a non-empty body");
    }

    S3BucketOptions bucketOptions = s3Options.effectiveOptionsForBucket(clientRequest.bucket());
    AwsCredentialsProvider credentialsProvider =
        S3Clients.awsCredentialsProvider(bucketOptions, secretsProvider, s3sessions);

    SdkHttpFullRequest.Builder request =
        SdkHttpFullRequest.builder()
            .uri(uri)
            .protocol(uri.getScheme())
            .method(SdkHttpMethod.fromValue(clientRequest.method()))
            .headers(clientRequest.headers());

    if (body.isEmpty()) {
      request.putHeader(SignerConstant.X_AMZ_CONTENT_SHA256, EMPTY_BODY_SHA256);
    } else {
      request.contentStreamProvider(() -> new ByteArrayInputStream(body.get().getBytes(UTF_8)));
    }

    AwsS3V4SignerParams params =
        AwsS3V4SignerParams.builder()
            .signingRegion(Region.of(clientRequest.region()))
            .signingName("s3")
            .awsCredentials(credentialsProvider.resolveCredentials())
            .enableChunkedEncoding(false)
            .timeOffset(0)
            .doubleUrlEncode(false)
            .enablePayloadSigning(false)
            .checksumParams(
                SignerChecksumParams.builder()
                    .algorithm(Algorithm.SHA256)
                    .isStreamingRequest(false)
                    .checksumHeaderName(SignerConstant.X_AMZ_CONTENT_SHA256)
                    .build())
            .build();

    SdkHttpFullRequest signed = signer.sign(request.build(), params);

    return ImmutableSigningResponse.of(signed.getUri(), signed.headers());
  }
}
