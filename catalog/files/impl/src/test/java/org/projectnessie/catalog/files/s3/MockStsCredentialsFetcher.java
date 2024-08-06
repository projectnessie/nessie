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

import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import software.amazon.awssdk.services.sts.model.Credentials;

class MockStsCredentialsFetcher implements StsCredentialsFetcher {

  final Supplier<Credentials> credentials;
  final AtomicInteger counter;

  MockStsCredentialsFetcher() {
    this(() -> Credentials.builder().build());
  }

  MockStsCredentialsFetcher(Supplier<Credentials> get) {
    this(get, new AtomicInteger());
  }

  MockStsCredentialsFetcher(Supplier<Credentials> credentials, AtomicInteger counter) {
    this.credentials = credentials;
    this.counter = counter;
  }

  @Override
  public Credentials fetchCredentialsForClient(
      S3BucketOptions bucketOptions, S3ClientIam iam, Optional<StorageLocations> locations) {
    counter.incrementAndGet();
    return credentials.get();
  }

  @Override
  public Credentials fetchCredentialsForServer(S3BucketOptions bucketOptions, S3ServerIam iam) {
    counter.incrementAndGet();
    return credentials.get();
  }
}
