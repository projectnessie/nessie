/*
 * Copyright (C) 2022 Dremio
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
package org.projectnessie.testing.floci.s3;

import com.google.common.base.Preconditions;
import io.floci.testcontainers.FlociContainer;
import java.net.URI;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import org.projectnessie.nessie.testing.containerspec.ContainerSpecHelper;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.Base58;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

public final class FlociS3Container extends FlociContainer implements FlociS3Access, AutoCloseable {

  private final String accessKey;
  private final String secretKey;
  private final String bucket;

  private String hostPort;
  private String s3endpoint;
  private S3Client s3;
  private String region;

  static boolean canRunOnMacOs() {
    return true;
  }

  @SuppressWarnings("unused")
  public FlociS3Container() {
    this(null, null, null, null);
  }

  public FlociS3Container(String image, String accessKey, String secretKey, String bucket) {
    super(
        ContainerSpecHelper.builder()
            .name("floci")
            .containerClass(FlociS3Container.class)
            .build()
            .dockerImageName(image)
            .asCompatibleSubstituteFor("floci/floci"));
    withNetworkAliases(randomString("floci"));
    withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger(FlociS3Container.class)));
    this.accessKey = accessKey != null ? accessKey : randomString("access");
    this.secretKey = secretKey != null ? secretKey : randomString("secret");
    this.bucket = bucket != null ? bucket : randomString("bucket");
  }

  @Override
  public FlociS3Container withRegion(String region) {
    this.region = region;
    super.withRegion(region);
    return this;
  }

  public FlociS3Container withIamEnforcement() {
    super.withIamConfig(c -> c.enforcementEnabled(true).seedDeployerPrincipal(true));
    return this;
  }

  @Override
  public FlociS3Container withDefaultAccountId(String accountId) {
    super.withDefaultAccountId(accountId);
    return this;
  }

  @Override
  public FlociS3Container withStartupAttempts(int attempts) {
    super.withStartupAttempts(attempts);
    return this;
  }

  private static String randomString(String prefix) {
    return prefix + "-" + Base58.randomString(6).toLowerCase(Locale.ROOT);
  }

  @Override
  public String hostPort() {
    Preconditions.checkState(hostPort != null, "Container not yet started");
    return hostPort;
  }

  @Override
  public String accessKey() {
    return accessKey;
  }

  @Override
  public String secretKey() {
    return secretKey;
  }

  @Override
  public String bucket() {
    return bucket;
  }

  @Override
  public String s3endpoint() {
    Preconditions.checkState(s3endpoint != null, "Container not yet started");
    return s3endpoint;
  }

  @Override
  public S3Client s3Client() {
    Preconditions.checkState(s3 != null, "Container not yet started");
    return s3;
  }

  @Override
  public Map<String, String> icebergProperties() {
    Map<String, String> props = new HashMap<>();
    props.put("s3.access-key-id", accessKey());
    props.put("s3.secret-access-key", secretKey());
    props.put("s3.endpoint", s3endpoint());
    props.put("s3.path-style-access", "true");
    props.put("http-client.type", "urlconnection");
    return props;
  }

  @Override
  public Map<String, String> hadoopConfig() {
    Map<String, String> r = new HashMap<>();
    r.put("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
    r.put("fs.s3a.access.key", accessKey());
    r.put("fs.s3a.secret.key", secretKey());
    r.put("fs.s3a.endpoint", s3endpoint());
    r.put("fs.s3a.path.style.access", "true");
    return r;
  }

  @Override
  public URI s3BucketUri(String path) {
    return s3BucketUri("s3", path);
  }

  public URI s3BucketUri(String scheme, String path) {
    Preconditions.checkState(bucket != null, "Container not yet started");
    return URI.create(String.format("%s://%s/", scheme, bucket)).resolve(path);
  }

  @Override
  public void start() {
    super.start();

    URI endpoint = URI.create(getEndpoint());
    this.hostPort = endpoint.getHost() + ":" + endpoint.getPort();
    this.s3endpoint = endpoint + "/";

    this.s3 = createS3Client();
    try {
      this.s3.createBucket(CreateBucketRequest.builder().bucket(bucket()).build());
    } catch (S3Exception e) {
      stop();
      throw e;
    }
  }

  @Override
  public void close() {
    stop();
  }

  @Override
  public void stop() {
    try {
      if (s3 != null) {
        s3.close();
      }
    } finally {
      s3 = null;
      super.stop();
    }
  }

  private S3Client createS3Client() {
    return S3Client.builder()
        .httpClientBuilder(UrlConnectionHttpClient.builder())
        .endpointOverride(URI.create(s3endpoint()))
        .forcePathStyle(true)
        .region(Region.of(region != null ? region : getRegion()))
        .credentialsProvider(
            StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey(), secretKey())))
        .build();
  }
}
