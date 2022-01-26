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
package org.projectnessie.jaxrs;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.projectnessie.model.Validation.HASH_MESSAGE;
import static org.projectnessie.model.Validation.REF_NAME_MESSAGE;
import static org.projectnessie.model.Validation.REF_NAME_OR_HASH_MESSAGE;

import org.assertj.core.api.Assumptions;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.projectnessie.client.http.HttpClient;
import org.projectnessie.client.http.HttpClientException;
import org.projectnessie.client.rest.NessieBadRequestException;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.Tag;

public abstract class AbstractRestInvalidWithHttp extends AbstractRestInvalidRefs {
  public static final String COMMA_VALID_HASH_1 =
      ",1234567890123456789012345678901234567890123456789012345678901234";
  public static final String COMMA_VALID_HASH_2 = ",1234567890123456789012345678901234567890";
  public static final String COMMA_VALID_HASH_3 = ",1234567890123456";

  protected abstract HttpClient getHttpClient();

  @ParameterizedTest
  @CsvSource({
    "x/" + COMMA_VALID_HASH_1,
    "abc'" + COMMA_VALID_HASH_1,
    ".foo" + COMMA_VALID_HASH_2,
    "abc'def'..'blah" + COMMA_VALID_HASH_2,
    "abc'de..blah" + COMMA_VALID_HASH_3,
    "abc'de@{blah" + COMMA_VALID_HASH_3
  })
  public void invalidBranchNames(String invalidBranchName, String validHash) {
    ContentKey key = ContentKey.of("x");
    Tag tag = Tag.of("valid", validHash);

    String opsCountMsg = ".operations.operations: size must be between 1 and 2147483647";

    assertAll(
        () ->
            assertThatThrownBy(
                    () ->
                        getApi()
                            .commitMultipleOperations()
                            .branchName(invalidBranchName)
                            .hash(validHash)
                            .commitMeta(CommitMeta.fromMessage(""))
                            .commit())
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessageContaining("Bad Request (HTTP/400):")
                .hasMessageContaining(REF_NAME_MESSAGE)
                .hasMessageContaining(opsCountMsg),
        () ->
            assertThatThrownBy(
                    () ->
                        getApi()
                            .deleteBranch()
                            .branchName(invalidBranchName)
                            .hash(validHash)
                            .delete())
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessageContaining("Bad Request (HTTP/400):")
                .hasMessageContaining(REF_NAME_MESSAGE),
        () ->
            assertThatThrownBy(
                    () ->
                        getApi()
                            .getCommitLog()
                            .refName(invalidBranchName)
                            .untilHash(validHash)
                            .get())
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessageContaining("Bad Request (HTTP/400):")
                .hasMessageContaining(REF_NAME_MESSAGE),
        () ->
            assertThatThrownBy(
                    () ->
                        getApi().getEntries().refName(invalidBranchName).hashOnRef(validHash).get())
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessageContaining("Bad Request (HTTP/400):")
                .hasMessageContaining(REF_NAME_MESSAGE),
        () ->
            assertThatThrownBy(() -> getApi().getReference().refName(invalidBranchName).get())
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessageContaining("Bad Request (HTTP/400):")
                .hasMessageContaining(REF_NAME_OR_HASH_MESSAGE),
        () ->
            assertThatThrownBy(
                    () ->
                        getApi()
                            .assignTag()
                            .tagName(invalidBranchName)
                            .hash(validHash)
                            .assignTo(tag)
                            .assign())
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessageContaining("Bad Request (HTTP/400):")
                .hasMessageContaining(REF_NAME_MESSAGE),
        () -> {
          if (null != getHttpClient()) {
            assertThatThrownBy(
                    () ->
                        getHttpClient()
                            .newRequest()
                            .path("trees/branch/{branchName}/merge")
                            .resolveTemplate("branchName", invalidBranchName)
                            .queryParam("expectedHash", validHash)
                            .post(null))
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessageContaining("Bad Request (HTTP/400):")
                .hasMessageContaining(REF_NAME_MESSAGE)
                .hasMessageContaining(".merge: must not be null");
          }
        },
        () ->
            assertThatThrownBy(
                    () ->
                        getApi()
                            .mergeRefIntoBranch()
                            .branchName(invalidBranchName)
                            .hash(validHash)
                            .fromRef(getApi().getDefaultBranch())
                            .merge())
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessageContaining("Bad Request (HTTP/400):")
                .hasMessageContaining(REF_NAME_MESSAGE),
        () ->
            assertThatThrownBy(
                    () -> getApi().deleteTag().tagName(invalidBranchName).hash(validHash).delete())
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessageContaining("Bad Request (HTTP/400):")
                .hasMessageContaining(REF_NAME_MESSAGE),
        () ->
            assertThatThrownBy(
                    () ->
                        getApi()
                            .transplantCommitsIntoBranch()
                            .branchName(invalidBranchName)
                            .hash(validHash)
                            .fromRefName("main")
                            .hashesToTransplant(
                                singletonList(
                                    getApi().getReference().refName("main").get().getHash()))
                            .transplant())
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessageContaining("Bad Request (HTTP/400):")
                .hasMessageContaining(REF_NAME_MESSAGE),
        () ->
            assertThatThrownBy(
                    () ->
                        getApi()
                            .getContent()
                            .key(key)
                            .refName(invalidBranchName)
                            .hashOnRef(validHash)
                            .get())
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessageContaining("Bad Request (HTTP/400):")
                .hasMessageContaining(REF_NAME_MESSAGE),
        () ->
            assertThatThrownBy(
                    () ->
                        getApi()
                            .getContent()
                            .key(key)
                            .refName(invalidBranchName)
                            .hashOnRef(validHash)
                            .get())
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessageContaining("Bad Request (HTTP/400):")
                .hasMessageContaining(REF_NAME_MESSAGE),
        () ->
            assertThatThrownBy(
                    () -> getApi().getDiff().fromRefName(invalidBranchName).toRefName("main").get())
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessageContaining("Bad Request (HTTP/400):")
                .hasMessageContaining(REF_NAME_MESSAGE));
  }

  @ParameterizedTest
  @CsvSource({
    "abc'" + COMMA_VALID_HASH_1,
    ".foo" + COMMA_VALID_HASH_2,
    "abc'def'..'blah" + COMMA_VALID_HASH_2,
    "abc'de..blah" + COMMA_VALID_HASH_3,
    "abc'de@{blah" + COMMA_VALID_HASH_3
  })
  public void invalidHashes(String invalidHashIn, String validHash) {
    // CsvSource maps an empty string as null
    String invalidHash = invalidHashIn != null ? invalidHashIn : "";

    String validBranchName = "hello";
    ContentKey key = ContentKey.of("x");
    Tag tag = Tag.of("valid", validHash);

    String opsCountMsg = ".operations.operations: size must be between 1 and 2147483647";

    assertAll(
        () ->
            assertThatThrownBy(
                    () ->
                        getApi()
                            .commitMultipleOperations()
                            .branchName(validBranchName)
                            .hash(invalidHash)
                            .commitMeta(CommitMeta.fromMessage(""))
                            .commit())
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessageContaining("Bad Request (HTTP/400):")
                .hasMessageContaining(".hash: " + HASH_MESSAGE)
                .hasMessageContaining(opsCountMsg),
        () ->
            assertThatThrownBy(
                    () ->
                        getApi()
                            .deleteBranch()
                            .branchName(validBranchName)
                            .hash(invalidHash)
                            .delete())
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessageContaining("Bad Request (HTTP/400):")
                .hasMessageContaining(".hash: " + HASH_MESSAGE),
        () ->
            assertThatThrownBy(
                    () ->
                        getApi()
                            .assignTag()
                            .tagName(validBranchName)
                            .hash(invalidHash)
                            .assignTo(tag)
                            .assign())
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessageContaining("Bad Request (HTTP/400):")
                .hasMessageContaining(".oldHash: " + HASH_MESSAGE),
        () -> {
          if (null != getHttpClient()) {
            assertThatThrownBy(
                    () ->
                        getHttpClient()
                            .newRequest()
                            .path("trees/branch/{branchName}/merge")
                            .resolveTemplate("branchName", validBranchName)
                            .queryParam("expectedHash", invalidHash)
                            .post(null))
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessageContaining("Bad Request (HTTP/400):")
                .hasMessageContaining("mergeRefIntoBranch.merge: must not be null")
                .hasMessageContaining(".hash: " + HASH_MESSAGE);
          }
        },
        () ->
            assertThatThrownBy(
                    () ->
                        getApi()
                            .mergeRefIntoBranch()
                            .branchName(validBranchName)
                            .hash(invalidHash)
                            .fromRef(getApi().getDefaultBranch())
                            .merge())
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessageContaining("Bad Request (HTTP/400):")
                .hasMessageContaining(".hash: " + HASH_MESSAGE),
        () ->
            assertThatThrownBy(
                    () -> getApi().deleteTag().tagName(validBranchName).hash(invalidHash).delete())
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessageContaining("Bad Request (HTTP/400):")
                .hasMessageContaining(".hash: " + HASH_MESSAGE),
        () ->
            assertThatThrownBy(
                    () ->
                        getApi()
                            .transplantCommitsIntoBranch()
                            .branchName(validBranchName)
                            .hash(invalidHash)
                            .fromRefName("main")
                            .hashesToTransplant(
                                singletonList(
                                    getApi().getReference().refName("main").get().getHash()))
                            .transplant())
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessageContaining("Bad Request (HTTP/400):")
                .hasMessageContaining(".hash: " + HASH_MESSAGE),
        () ->
            assertThatThrownBy(() -> getApi().getContent().refName(invalidHash).get())
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessageContaining("Bad Request (HTTP/400):")
                .hasMessageContaining(
                    ".request.requestedKeys: size must be between 1 and 2147483647")
                .hasMessageContaining(REF_NAME_MESSAGE),
        () ->
            assertThatThrownBy(
                    () ->
                        getApi().getContent().refName(validBranchName).hashOnRef(invalidHash).get())
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessageContaining("Bad Request (HTTP/400):")
                .hasMessageContaining(
                    ".request.requestedKeys: size must be between 1 and 2147483647")
                .hasMessageContaining(".hashOnRef: " + HASH_MESSAGE),
        () ->
            assertThatThrownBy(
                    () ->
                        getApi()
                            .getContent()
                            .key(key)
                            .refName(validBranchName)
                            .hashOnRef(invalidHash)
                            .get())
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessageContaining("Bad Request (HTTP/400):")
                .hasMessageContaining(".hashOnRef: " + HASH_MESSAGE),
        () ->
            assertThatThrownBy(
                    () ->
                        getApi()
                            .getCommitLog()
                            .refName(validBranchName)
                            .untilHash(invalidHash)
                            .get())
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessageContaining("Bad Request (HTTP/400):")
                .hasMessageContaining(".params.startHash: " + HASH_MESSAGE),
        () ->
            assertThatThrownBy(
                    () ->
                        getApi()
                            .getCommitLog()
                            .refName(validBranchName)
                            .hashOnRef(invalidHash)
                            .get())
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessageContaining("Bad Request (HTTP/400):")
                .hasMessageContaining(".params.endHash: " + HASH_MESSAGE),
        () ->
            assertThatThrownBy(
                    () ->
                        getApi().getEntries().refName(validBranchName).hashOnRef(invalidHash).get())
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessageContaining("Bad Request (HTTP/400):")
                .hasMessageContaining(".params.hashOnRef: " + HASH_MESSAGE));
  }

  @ParameterizedTest
  @CsvSource({
    "" + COMMA_VALID_HASH_1,
    "abc'" + COMMA_VALID_HASH_1,
    ".foo" + COMMA_VALID_HASH_2,
    "abc'def'..'blah" + COMMA_VALID_HASH_2,
    "abc'de..blah" + COMMA_VALID_HASH_3,
    "abc'de@{blah" + COMMA_VALID_HASH_3
  })
  public void invalidTags(String invalidTagNameIn, String validHash) {
    Assumptions.assumeThat(getHttpClient()).isNotNull();
    // CsvSource maps an empty string as null
    String invalidTagName = invalidTagNameIn != null ? invalidTagNameIn : "";

    String validBranchName = "hello";
    // Need the string-ified JSON representation of `Tag` here, because `Tag` itself performs
    // validation.
    String tag =
        "{\"type\": \"TAG\", \"name\": \""
            + invalidTagName
            + "\", \"hash\": \""
            + validHash
            + "\"}";
    String branch =
        "{\"type\": \"BRANCH\", \"name\": \""
            + invalidTagName
            + "\", \"hash\": \""
            + validHash
            + "\"}";
    String different =
        "{\"type\": \"FOOBAR\", \"name\": \""
            + invalidTagName
            + "\", \"hash\": \""
            + validHash
            + "\"}";
    assertAll(
        () ->
            assertThatThrownBy(
                    () ->
                        unwrap(
                            () ->
                                getHttpClient()
                                    .newRequest()
                                    .path("trees/tag/{tagName}")
                                    .resolveTemplate("tagName", validBranchName)
                                    .queryParam("expectedHash", validHash)
                                    .put(null)))
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessageStartingWith("Bad Request (HTTP/400):")
                .hasMessageContaining(".assignTo: must not be null"),
        () ->
            assertThatThrownBy(
                    () ->
                        unwrap(
                            () ->
                                getHttpClient()
                                    .newRequest()
                                    .path("trees/tag/{tagName}")
                                    .resolveTemplate("tagName", validBranchName)
                                    .queryParam("expectedHash", validHash)
                                    .put(tag)))
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessageStartingWith(
                    "Bad Request (HTTP/400): Cannot construct instance of "
                        + "`org.projectnessie.model.ImmutableTag`, problem: "
                        + REF_NAME_MESSAGE
                        + " - but was: "
                        + invalidTagName
                        + "\n"),
        () ->
            assertThatThrownBy(
                    () ->
                        unwrap(
                            () ->
                                getHttpClient()
                                    .newRequest()
                                    .path("trees/tag/{tagName}")
                                    .resolveTemplate("tagName", validBranchName)
                                    .queryParam("expectedHash", validHash)
                                    .put(branch)))
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessageStartingWith("Bad Request (HTTP/400): Cannot construct instance of ")
                .hasMessageContaining(REF_NAME_MESSAGE),
        () ->
            assertThatThrownBy(
                    () ->
                        unwrap(
                            () ->
                                getHttpClient()
                                    .newRequest()
                                    .path("trees/tag/{tagName}")
                                    .resolveTemplate("tagName", validBranchName)
                                    .queryParam("expectedHash", validHash)
                                    .put(different)))
                .isInstanceOf(NessieBadRequestException.class)
                .hasMessageStartingWith(
                    "Bad Request (HTTP/400): Could not resolve type id 'FOOBAR' as a subtype of "
                        + "`org.projectnessie.model.Reference`: known type ids = ["));
  }

  void unwrap(Executable exec) throws Throwable {
    try {
      exec.execute();
    } catch (Throwable targetException) {
      if (targetException instanceof HttpClientException) {
        if (targetException.getCause() instanceof NessieNotFoundException
            || targetException.getCause() instanceof NessieConflictException) {
          throw targetException.getCause();
        }
      }

      throw targetException;
    }
  }
}
