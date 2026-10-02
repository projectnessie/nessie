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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.projectnessie.catalog.service.objtypes.SignerKey;

@ExtendWith(SoftAssertionsExtension.class)
public class TestSignerSignature {
  @InjectSoftAssertions protected SoftAssertions soft;

  /**
   * Signer key, signature attributes, signature and path parameter of a token minted with the
   * {@link SignerSignature} before {@link SignerSignature#pathStyleAccess()} was added. The golden
   * values must never be regenerated: they prove that tokens in flight across an upgrade still
   * verify.
   */
  static final Instant GOLDEN_KEY_CREATED = Instant.ofEpochSecond(1_700_000_000L);

  static final SignerKey GOLDEN_KEY =
      SignerKey.builder()
          .name("key")
          .secretKey("01234567890123456789012345678901".getBytes(UTF_8))
          .creationTime(GOLDEN_KEY_CREATED)
          .rotationTime(GOLDEN_KEY_CREATED.plus(3, DAYS))
          .expirationTime(GOLDEN_KEY_CREATED.plus(5, DAYS))
          .build();

  static final SignerSignature GOLDEN_SIGNATURE_ATTRIBUTES =
      SignerSignature.builder()
          .prefix("prefix")
          .identifier("my.namespaced.table")
          .warehouseLocation("s3://foo-bucket/")
          .expirationTimestamp(4_102_444_800L)
          .addWriteLocation("s3://foo-bucket/write/here/")
          .addReadLocation("s3://other/read-there/")
          .build();

  static final String GOLDEN_SIGNATURE =
      "75a43f8e0d8f338826ad949b842e10669a0f4607697b17b2c2fa8037a708a365";

  static final String GOLDEN_PATH_PARAM =
      "H4sIAAAAAAAA_12OvU7DMBRGxcrESzAwtHbj1HYqsSCxIZD4Wdiu42t6VRJHtqO2EywgkAAhnhbahTQVIDF9wzk6"
          + "-iYHuzurxxkuT6HCo26fIt3UkNqAd2oMuXAaudVOCK0zCbbIC6PzDEdcygK4yyVXslBmpExWZg40FwoU1yDk"
          + "-G2TwnDxE1w9NAEdLY6380wW60SOMJxXy2HdHYgNlGiHCcwtvs8h4NS3EU98CYl8fRbFhDHn_cC05QwTe50H"
          + "Sr84fl7_F3rOphiQfb0EBPvnXvWuTx1kGzJIW-0DFw2FXrqk7lGCqtk_3EN9v15_A_8cHDctAQAA";

  @Test
  public void tokenMintedBeforePathStyleAccessStillVerifies() {
    Instant now = GOLDEN_KEY_CREATED.plus(1, HOURS);

    SignerParams signerParams = SignerParams.fromPathParam(GOLDEN_PATH_PARAM);

    soft.assertThat(signerParams.keyName()).isEqualTo(GOLDEN_KEY.name());
    soft.assertThat(signerParams.signature()).isEqualTo(GOLDEN_SIGNATURE);
    soft.assertThat(signerParams.signerSignature()).isEqualTo(GOLDEN_SIGNATURE_ATTRIBUTES);
    soft.assertThat(signerParams.signerSignature().pathStyleAccess()).isEmpty();
    soft.assertThat(signerParams.signerSignature().verify(GOLDEN_KEY, GOLDEN_SIGNATURE, now))
        .isEmpty();

    // Tokens without pathStyleAccess are minted byte-for-byte as before.
    soft.assertThat(GOLDEN_SIGNATURE_ATTRIBUTES.sign(GOLDEN_KEY)).isEqualTo(GOLDEN_SIGNATURE);
    soft.assertThat(GOLDEN_SIGNATURE_ATTRIBUTES.toPathParam(GOLDEN_KEY))
        .isEqualTo(GOLDEN_PATH_PARAM);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void pathStyleAccessIsSigned(boolean pathStyleAccess) {
    Instant now = GOLDEN_KEY_CREATED.plus(1, HOURS);

    SignerSignature withPathStyleAccess =
        SignerSignature.builder()
            .from(GOLDEN_SIGNATURE_ATTRIBUTES)
            .pathStyleAccess(pathStyleAccess)
            .build();
    String signature = withPathStyleAccess.sign(GOLDEN_KEY);

    soft.assertThat(signature).isNotEqualTo(GOLDEN_SIGNATURE);
    soft.assertThat(withPathStyleAccess.verify(GOLDEN_KEY, signature, now)).isEmpty();

    SignerParams signerParams =
        SignerParams.fromPathParam(withPathStyleAccess.toPathParam(GOLDEN_KEY));
    soft.assertThat(signerParams.signerSignature()).isEqualTo(withPathStyleAccess);
    soft.assertThat(signerParams.signerSignature().verify(GOLDEN_KEY, signature, now)).isEmpty();

    // Neither dropping nor flipping the attribute keeps the signature valid.
    soft.assertThat(GOLDEN_SIGNATURE_ATTRIBUTES.verify(GOLDEN_KEY, signature, now))
        .contains("Got invalid signature");
    soft.assertThat(
            SignerSignature.builder()
                .from(withPathStyleAccess)
                .pathStyleAccess(!pathStyleAccess)
                .build()
                .verify(GOLDEN_KEY, signature, now))
        .contains("Got invalid signature");
    // Adding the attribute to a token without it does not keep the signature valid either.
    soft.assertThat(withPathStyleAccess.verify(GOLDEN_KEY, GOLDEN_SIGNATURE, now))
        .contains("Got invalid signature");
  }

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
