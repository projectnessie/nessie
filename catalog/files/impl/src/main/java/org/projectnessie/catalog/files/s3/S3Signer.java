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
import org.projectnessie.catalog.files.config.S3BucketOptions;
import org.projectnessie.catalog.files.config.S3Options;
import org.projectnessie.catalog.secrets.SecretsProvider;
import org.projectnessie.storage.uri.StorageUri;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.http.ContentStreamProvider;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.auth.aws.signer.AwsV4HttpSigner;
import software.amazon.awssdk.http.auth.spi.signer.SignRequest;
import software.amazon.awssdk.http.auth.spi.signer.SignedRequest;
import software.amazon.awssdk.identity.spi.AwsCredentialsIdentity;
import software.amazon.awssdk.services.s3.S3Client;

public class S3Signer implements RequestSigner {

  private final AwsV4HttpSigner signer = AwsV4HttpSigner.create();
  private final S3Options s3Options;
  private final SecretsProvider secretsProvider;
  private final S3Sessions s3sessions;

  public S3Signer(S3Options s3Options, SecretsProvider secretsProvider, S3Sessions s3sessions) {
    this.s3Options = s3Options;
    this.secretsProvider = secretsProvider;
    this.s3sessions = s3sessions;
  }

  @Override
  public SigningResponse sign(SigningRequest clientRequest) {

    URI uri = clientRequest.uri();
    Optional<String> body = clientRequest.body();

    if (body.isEmpty()
        && "post".equalsIgnoreCase(clientRequest.method())
        && uri.getQuery().contains("delete")) {
      throw new IllegalArgumentException("DELETE requests must have a non-empty body");
    }

    SdkHttpFullRequest.Builder request =
        SdkHttpFullRequest.builder()
            .uri(uri)
            .protocol(uri.getScheme())
            .method(SdkHttpMethod.fromValue(clientRequest.method()))
            .headers(clientRequest.headers());

    S3BucketOptions bucketOptions =
        s3Options.resolveOptionsForUri(
            StorageUri.of(S3Utils.asS3Location(clientRequest.uri().toString())));
    AwsCredentialsProvider credentialsProvider =
        S3Clients.serverCredentialsProvider(bucketOptions, s3sessions, secretsProvider);
    AwsCredentialsIdentity credentials = credentialsProvider.resolveCredentials();

    SignRequest.Builder<AwsCredentialsIdentity> signRequest =
        SignRequest.builder(credentials)
            .request(request.build())
            .putProperty(AwsV4HttpSigner.REGION_NAME, clientRequest.region())
            .putProperty(AwsV4HttpSigner.SERVICE_SIGNING_NAME, S3Client.SERVICE_NAME)
            .putProperty(AwsV4HttpSigner.DOUBLE_URL_ENCODE, false)
            .putProperty(AwsV4HttpSigner.NORMALIZE_PATH, false)
            .putProperty(AwsV4HttpSigner.CHUNK_ENCODING_ENABLED, false)
            .putProperty(AwsV4HttpSigner.PAYLOAD_SIGNING_ENABLED, false);

    body.map(
            s -> (ContentStreamProvider) () -> new ByteArrayInputStream(body.get().getBytes(UTF_8)))
        .ifPresent(signRequest::payload);
    // Eventually refactor the above line to this one:
    // body.map(ContentStreamProvider::fromUtf8String).ifPresent(signRequest::payload);

    SignedRequest signed = signer.sign(signRequest.build());
    SdkHttpRequest signedRequest = signed.request();

    return ImmutableSigningResponse.of(signedRequest.getUri(), signedRequest.headers());
  }
}
