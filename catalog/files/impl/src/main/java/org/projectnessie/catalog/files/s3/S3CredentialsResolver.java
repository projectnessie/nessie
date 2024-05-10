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

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import software.amazon.awssdk.auth.credentials.AwsCredentials;

public class S3CredentialsResolver {

  private final Clock clock;
  private final S3Sessions sessions;

  public S3CredentialsResolver(Clock clock, S3Sessions sessions) {
    this.clock = clock;
    this.sessions = sessions;
  }

  public S3Credentials resolveSessionCredentials(S3BucketOptions bucketOptions) {
    AwsCredentials credentials = sessions.assumeRoleForClient(bucketOptions).resolveCredentials();

    // Make sure the received credentials are actually valid until the expected session end.
    Optional<Instant> expirationInstant = credentials.expirationTime();
    if (expirationInstant.isPresent()) {
      Instant now = clock.instant();
      // Note: expiry instance accuracy in STS is seconds.
      Duration requiredDuration = bucketOptions.minSessionCredentialValidityPeriod();
      Instant sessionEnd = now.plus(requiredDuration).truncatedTo(ChronoUnit.SECONDS);
      checkArgument(
          !sessionEnd.isAfter(expirationInstant.get()),
          "Provided credentials expire (%s) before the expected session end (now: %s, duration: %s)",
          expirationInstant.get(),
          now,
          requiredDuration);
    }

    return new S3Credentials(credentials);
  }
}
