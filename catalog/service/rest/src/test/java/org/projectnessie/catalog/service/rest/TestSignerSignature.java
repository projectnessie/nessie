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
package org.projectnessie.catalog.service.rest;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.Clock.systemUTC;
import static java.time.temporal.ChronoUnit.DAYS;
import static java.time.temporal.ChronoUnit.HOURS;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.catalog.service.objtypes.SignerKey;

@ExtendWith(SoftAssertionsExtension.class)
public class TestSignerSignature {
  @InjectSoftAssertions protected SoftAssertions soft;

  @Test
  public void signAndVerify() {
    Instant now = Instant.now();

    SignerKey key =
        SignerKey.builder()
            .name("key")
            .secretKey("01234567890123456789012345678901".getBytes(UTF_8))
            .creationTime(now)
            .rotationTime(now.plus(3, DAYS))
            .expirationTime(now.plus(5, DAYS))
            .build();

    long expirationTimestamp = systemUTC().instant().plus(3, ChronoUnit.HOURS).getEpochSecond();

    SignerSignature signerSignature =
        SignerSignature.builder()
            .prefix("prefix")
            .identifier("my.namespaced.table")
            .warehouseLocation("s3://foo-bucket/")
            .expirationTimestamp(expirationTimestamp)
            .addWriteLocation("s3://foo-bucket/write/here/")
            .addWriteLocation("s3://foo-bucket/write-here-as-well/")
            .addReadLocation("s3://other/read-there/")
            .build();

    String signature = signerSignature.sign(key);
    soft.assertThat(signerSignature.verify(key, signature, now)).isEmpty();

    String pathParam = signerSignature.toPathParam(key);
    SignerParams signerParams = SignerParams.fromPathParam(pathParam);
    soft.assertThat(signerParams)
        .extracting(SignerParams::keyName, SignerParams::signature, SignerParams::signerSignature)
        .containsExactly(key.name(), signature, signerSignature);

    soft.assertThat(signerSignature.verify(null, signature, now))
        .contains("Could not find signingKey");
    soft.assertThat(signerSignature.verify(key, "tampered-signature", now))
        .contains("Got invalid signature");
    soft.assertThat(signerSignature.verify(key, signature, now.plus(3, HOURS)))
        .contains("Got expired signature");

    soft.assertThat(
            SignerSignature.builder()
                .from(signerSignature)
                .prefix("not-the-prefix")
                .build()
                .verify(key, signature, now))
        .contains("Got invalid signature");
    soft.assertThat(
            SignerSignature.builder()
                .from(signerSignature)
                .identifier("not-the-table")
                .build()
                .verify(key, signature, now))
        .contains("Got invalid signature");
    soft.assertThat(
            SignerSignature.builder()
                .from(signerSignature)
                .warehouseLocation("not-the-warehouse")
                .build()
                .verify(key, signature, now))
        .contains("Got invalid signature");
    soft.assertThat(
            SignerSignature.builder()
                .from(signerSignature)
                .expirationTimestamp(Long.MAX_VALUE)
                .build()
                .verify(key, signature, now))
        .contains("Got invalid signature");
    soft.assertThat(
            SignerSignature.builder()
                .from(signerSignature)
                .addWriteLocation("s3://secret/stuff/")
                .build()
                .verify(key, signature, now))
        .contains("Got invalid signature");
    soft.assertThat(
            SignerSignature.builder()
                .from(signerSignature)
                .addReadLocation("s3://secret/stuff/")
                .build()
                .verify(key, signature, now))
        .contains("Got invalid signature");
    soft.assertThat(
            SignerSignature.builder()
                .from(signerSignature)
                .writeLocations(List.of())
                .build()
                .verify(key, signature, now))
        .contains("Got invalid signature");
    soft.assertThat(
            SignerSignature.builder()
                .from(signerSignature)
                .readLocations(List.of())
                .build()
                .verify(key, signature, now))
        .contains("Got invalid signature");
  }
}
