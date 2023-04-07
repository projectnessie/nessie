/*
 * Copyright (C) 2023 Dremio
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
package org.projectnessie.client.auth.oauth2;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.time.Instant;
import java.util.Base64;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import javax.annotation.Nullable;

/**
 * A minimal representation of a JWT token, as defined in <a
 * href="https://datatracker.ietf.org/doc/html/rfc7519">RFC 7519</a>.
 *
 * <p>The current implementation only supports extracting the payload claims. It does not decode nor
 * validate the header and the signature. It is not particularly efficient, but is suitable for
 * access by multiple threads.
 */
class JwtToken {

  public static final String EXP_CLAIM = "exp";
  public static final String ISS_CLAIM = "iss";
  public static final String SUB_CLAIM = "sub";
  public static final String AUD_CLAIM = "aud";
  public static final String NBF_CLAIM = "nbf";
  public static final String IAT_CLAIM = "iat";
  public static final String JTI_CLAIM = "jti";

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  /**
   * Parses a JWT token from a string.
   *
   * @param token The JWT token to parse.
   * @return The parsed {@link JwtToken}.
   * @throws IllegalArgumentException if the token is invalid.
   */
  public static JwtToken parse(String token) {
    if (token != null) {
      @SuppressWarnings("StringSplitter")
      String[] parts = token.split("\\.");
      if (parts.length == 3 && parts[1].length() > 0) {
        try {
          JsonNode payload = OBJECT_MAPPER.readTree(Base64.getUrlDecoder().decode(parts[1]));
          return new JwtToken(payload);
        } catch (IOException ignored) {
          // fall-through
        }
      }
    }
    throw new IllegalArgumentException("Invalid JWT token: " + token);
  }

  private final JsonNode payload;
  private final ConcurrentMap<String, Object> claims;

  private JwtToken(JsonNode payload) {
    this.payload = payload;
    claims = new ConcurrentHashMap<>();
  }

  /** Returns the raw payload of the JWT token. */
  public JsonNode getPayload() {
    return payload;
  }

  /**
   * The "iss" (issuer) claim identifies the principal that issued the JWT. The processing of this
   * claim is generally application specific. The "iss" value is a case-sensitive string containing
   * a StringOrURI value. Use of this claim is OPTIONAL.
   */
  @Nullable
  public String getIssuer() {
    return (String)
        claims.computeIfAbsent(
            ISS_CLAIM,
            k ->
                payload.has(ISS_CLAIM) && payload.get(ISS_CLAIM).isTextual()
                    ? payload.get(ISS_CLAIM).asText()
                    : null);
  }

  /**
   * The "sub" (subject) claim identifies the principal that is the subject of the JWT. The claims
   * in a JWT are normally statements about the subject. The subject value MUST either be scoped to
   * be locally unique in the context of the issuer or be globally unique. The processing of this
   * claim is generally application specific. The "sub" value is a case-sensitive string containing
   * a StringOrURI value. Use of this claim is OPTIONAL.
   */
  @Nullable
  public String getSubject() {
    return (String)
        claims.computeIfAbsent(
            SUB_CLAIM,
            k ->
                payload.has(SUB_CLAIM) && payload.get(SUB_CLAIM).isTextual()
                    ? payload.get(SUB_CLAIM).asText()
                    : null);
  }

  /**
   * The "aud" (audience) claim identifies the recipients that the JWT is intended for. Each
   * principal intended to process the JWT MUST identify itself with a value in the audience claim.
   * If the principal processing the claim does not identify itself with a value in the "aud" claim
   * when this claim is present, then the JWT MUST be rejected. In the general case, the "aud" value
   * is an array of case- sensitive strings, each containing a StringOrURI value. In the special
   * case when the JWT has one audience, the "aud" value MAY be a single case-sensitive string
   * containing a StringOrURI value. The interpretation of audience values is generally application
   * specific. Use of this claim is OPTIONAL.
   */
  @Nullable
  public String getAudience() {
    return (String)
        claims.computeIfAbsent(
            AUD_CLAIM,
            k ->
                payload.has(AUD_CLAIM) && payload.get(AUD_CLAIM).isTextual()
                    ? payload.get(AUD_CLAIM).asText()
                    : null);
  }

  /**
   * The "exp" (expiration time) claim identifies the expiration time on or after which the JWT MUST
   * NOT be accepted for processing. The processing of the "exp" claim requires that the current
   * date/time MUST be before the expiration date/time listed in the "exp" claim.
   *
   * <p>Implementers MAY provide for some small leeway, usually no more than a few minutes, to
   * account for clock skew. Its value MUST be a number containing a <a
   * href="https://datatracker.ietf.org/doc/html/rfc7519#section-2">NumericDate</a> value. Use of
   * this claim is OPTIONAL.
   */
  @Nullable
  public Instant getExpirationTime() {
    return (Instant)
        claims.computeIfAbsent(
            EXP_CLAIM,
            k ->
                payload.has(EXP_CLAIM) && payload.get(EXP_CLAIM).canConvertToLong()
                    ? Instant.ofEpochSecond(payload.get(EXP_CLAIM).asLong())
                    : null);
  }

  /**
   * The "nbf" (not before) claim identifies the time before which the JWT MUST NOT be accepted for
   * processing. The processing of the "nbf" claim requires that the current date/time MUST be after
   * or equal to the not-before date/time listed in the "nbf" claim. Implementers MAY provide for
   * some small leeway, usually no more than a few minutes, to account for clock skew. Its value
   * MUST be a number containing a NumericDate value. Use of this claim is OPTIONAL.
   */
  @Nullable
  public Instant getNotBefore() {
    return (Instant)
        claims.computeIfAbsent(
            NBF_CLAIM,
            k ->
                payload.has(NBF_CLAIM) && payload.get(NBF_CLAIM).canConvertToLong()
                    ? Instant.ofEpochSecond(payload.get(NBF_CLAIM).asLong())
                    : null);
  }

  /**
   * The "iat" (issued at) claim identifies the time at which the JWT was issued. This claim can be
   * used to determine the age of the JWT. Its value MUST be a number containing a NumericDate
   * value. Use of this claim is OPTIONAL.
   */
  @Nullable
  public Instant getIssuedAt() {
    return (Instant)
        claims.computeIfAbsent(
            IAT_CLAIM,
            k ->
                payload.has(IAT_CLAIM) && payload.get(IAT_CLAIM).canConvertToLong()
                    ? Instant.ofEpochSecond(payload.get(IAT_CLAIM).asLong())
                    : null);
  }

  /**
   * The "jti" (JWT ID) claim provides a unique identifier for the JWT. The identifier value MUST be
   * assigned in a manner that ensures that there is a negligible probability that the same value
   * will be accidentally assigned to a different data object; if the application uses multiple
   * issuers, collisions MUST be prevented among values produced by different issuers as well. The
   * "jti" claim can be used to prevent the JWT from being replayed. The "jti" value is a case-
   * sensitive string. Use of this claim is OPTIONAL.
   */
  @Nullable
  public String getId() {
    return (String)
        claims.computeIfAbsent(
            JTI_CLAIM,
            k ->
                payload.has(JTI_CLAIM) && payload.get(JTI_CLAIM).isTextual()
                    ? payload.get(JTI_CLAIM).asText()
                    : null);
  }

  @Override
  public String toString() {
    return "JwtToken" + payload;
  }
}
