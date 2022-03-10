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

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.collect.ImmutableMap;
import java.net.URI;
import java.util.Locale;
import java.util.UUID;
import javax.annotation.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.client.http.HttpClient;
import org.projectnessie.client.http.HttpClientBuilder;
import org.projectnessie.client.rest.NessieHttpResponseFilter;
import org.projectnessie.error.BaseNessieClientServerException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.Detached;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.Operation.Put;
import org.projectnessie.model.Reference;
import org.projectnessie.model.Tag;

/** See {@link AbstractTestRest} for details about and reason for the inheritance model. */
public abstract class AbstractRest {

  private NessieApiV1 api;
  private HttpClient httpClient;
  private URI uri;

  static {
    // Note: REST tests validate some locale-specific error messages, but expect on the messages to
    // be in ENGLISH. However, the JRE's startup classes (in particular class loaders) may cause the
    // default Locale to be initialized before Maven is able to override the user.language system
    // property. Therefore, we explicitly set the default Locale to ENGLISH here to match tests'
    // expectations.
    Locale.setDefault(Locale.ENGLISH);
  }

  protected void init(URI uri) {
    NessieApiV1 api = HttpClientBuilder.builder().withUri(uri).build(NessieApiV1.class);

    ObjectMapper mapper =
        new ObjectMapper()
            .enable(SerializationFeature.INDENT_OUTPUT)
            .disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
    HttpClient.Builder httpClient = HttpClient.builder().setBaseUri(uri).setObjectMapper(mapper);
    httpClient.addResponseFilter(new NessieHttpResponseFilter(mapper));

    init(api, httpClient, uri);
  }

  protected void init(NessieApiV1 api, @Nullable HttpClient.Builder httpClient, URI uri) {
    this.api = api;
    this.httpClient = httpClient != null ? httpClient.build() : null;
    this.uri = uri;
  }

  @BeforeEach
  public void setUp() {
    init(URI.create("http://localhost:19121/api/v1"));
  }

  @AfterEach
  public void tearDown() throws Exception {
    Branch defaultBranch = api.getDefaultBranch();
    for (Reference ref : api.getAllReferences().get().getReferences()) {
      if (ref.getName().equals(defaultBranch.getName())) {
        continue;
      }
      if (ref instanceof Branch) {
        api.deleteBranch().branch((Branch) ref).delete();
      } else if (ref instanceof Tag) {
        api.deleteTag().tag((Tag) ref).delete();
      }
    }

    api.close();
  }

  public NessieApiV1 getApi() {
    return api;
  }

  public HttpClient getHttpClient() {
    return httpClient;
  }

  public URI getUri() {
    return uri;
  }

  protected String createCommits(
      Reference branch, int numAuthors, int commitsPerAuthor, String currentHash)
      throws BaseNessieClientServerException {
    for (int j = 0; j < numAuthors; j++) {
      String author = "author-" + j;
      for (int i = 0; i < commitsPerAuthor; i++) {
        IcebergTable meta =
            IcebergTable.of(UUID.randomUUID().toString(), "some-file-" + i, 42, 42, 42, 42);
        String nextHash =
            getApi()
                .commitMultipleOperations()
                .branchName(branch.getName())
                .hash(currentHash)
                .commitMeta(
                    CommitMeta.builder()
                        .author(author)
                        .message("committed-by-" + author)
                        .properties(ImmutableMap.of("prop1", "val1", "prop2", "val2"))
                        .build())
                .operation(Put.of(ContentKey.of("table" + i), meta))
                .commit()
                .getHash();
        assertThat(currentHash).isNotEqualTo(nextHash);
        currentHash = nextHash;
      }
    }
    return currentHash;
  }

  protected Branch createBranch(String name, Branch from) throws BaseNessieClientServerException {
    Branch expectedBranch;
    String srcBranchName;
    if (from == null) {
      Branch main = getApi().getDefaultBranch();
      expectedBranch = Branch.of(name, main.getHash());
      srcBranchName = "main";
    } else {
      expectedBranch = Branch.of(name, from.getHash());
      srcBranchName = from.getName();
    }
    Reference created =
        getApi()
            .createReference()
            .sourceRefName(srcBranchName)
            .reference(Branch.of(name, expectedBranch.getHash()))
            .create();
    assertThat(created).isEqualTo(expectedBranch);
    return expectedBranch;
  }

  protected Branch createBranch(String name) throws BaseNessieClientServerException {
    return createBranch(name, null);
  }

  protected void deleteBranch(String name, String hash) throws BaseNessieClientServerException {
    getApi().deleteBranch().branchName(name).hash(hash).delete();
  }

  /**
   * Used by parameterized tests to return the {@value Detached#REF_NAME}, if {@code
   * withDetachedCommit} is {@code true} or the {@link Reference#getName() reference name} from the
   * given {@code ref}.
   */
  protected static String maybeAsDetachedName(boolean withDetachedCommit, Reference ref) {
    return withDetachedCommit ? Detached.REF_NAME : ref.getName();
  }

  /**
   * Enum intended to be used a test method parameter to transform a {@link Reference} in multiple
   * ways.
   */
  enum ReferenceMode {
    /** Removes the {@link Reference#getHash()} from the reference. */
    NAME_ONLY {
      @Override
      Reference transform(Reference ref) {
        switch (ref.getType()) {
          case TAG:
            return Tag.of(ref.getName(), null);
          case BRANCH:
            return Branch.of(ref.getName(), null);
          default:
            throw new IllegalArgumentException(ref.toString());
        }
      }
    },
    /** Keep the reference unchanged. */
    UNCHANGED {
      @Override
      Reference transform(Reference ref) {
        return ref;
      }
    },
    /**
     * Make the reference a {@link Detached} with its {@link Detached#getHash()} using the hash of
     * the given reference.
     */
    DETACHED {
      @Override
      Reference transform(Reference ref) {
        return Detached.of(ref.getHash());
      }
    };

    abstract Reference transform(Reference ref);
  }
}
