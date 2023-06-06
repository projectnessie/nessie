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
package org.projectnessie.client.builder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Stream;
import org.projectnessie.client.api.GetDiffBuilder;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.DiffResponse;

public abstract class BaseGetDiffBuilder<PARAMS> implements GetDiffBuilder {

  private final BiFunction<PARAMS, String, PARAMS> paramsForPage;
  private String pageToken;

  protected Integer maxRecords;
  protected String fromRefName;
  protected String fromHashOnRef;
  protected String toRefName;
  protected String toHashOnRef;
  protected final List<ContentKey> keys = new ArrayList<>();
  protected ContentKey minKey;
  protected ContentKey maxKey;
  protected ContentKey prefixKey;
  protected String filter;

  protected BaseGetDiffBuilder(BiFunction<PARAMS, String, PARAMS> paramsForPage) {
    this.paramsForPage = paramsForPage;
  }

  @Override
  public GetDiffBuilder fromRefName(String fromRefName) {
    this.fromRefName = fromRefName;
    return this;
  }

  @Override
  public GetDiffBuilder fromHashOnRef(String fromHashOnRef) {
    this.fromHashOnRef = fromHashOnRef;
    return this;
  }

  @Override
  public GetDiffBuilder toRefName(String toRefName) {
    this.toRefName = toRefName;
    return this;
  }

  @Override
  public GetDiffBuilder toHashOnRef(String toHashOnRef) {
    this.toHashOnRef = toHashOnRef;
    return this;
  }

  @Override
  public GetDiffBuilder maxRecords(int maxRecords) {
    this.maxRecords = maxRecords;
    return this;
  }

  @Override
  public GetDiffBuilder pageToken(String pageToken) {
    this.pageToken = pageToken;
    return this;
  }

  @Override
  public GetDiffBuilder key(ContentKey key) {
    this.keys.add(key);
    return this;
  }

  @Override
  public GetDiffBuilder keys(Collection<ContentKey> keys) {
    this.keys.addAll(keys);
    return this;
  }

  @Override
  public GetDiffBuilder minKey(ContentKey minKey) {
    this.minKey = minKey;
    return this;
  }

  @Override
  public GetDiffBuilder maxKey(ContentKey maxKey) {
    this.maxKey = maxKey;
    return this;
  }

  @Override
  public GetDiffBuilder prefixKey(ContentKey prefixKey) {
    this.prefixKey = prefixKey;
    return this;
  }

  @Override
  public GetDiffBuilder filter(String filter) {
    this.filter = filter;
    return this;
  }

  @Override
  public DiffResponse get() throws NessieNotFoundException {
    return get(paramsForPage.apply(params(), pageToken));
  }

  protected abstract PARAMS params();

  protected abstract DiffResponse get(PARAMS p) throws NessieNotFoundException;

  @Override
  public Stream<DiffResponse.DiffEntry> stream() throws NessieNotFoundException {
    PARAMS p = params();
    return StreamingUtil.generateStream(
        DiffResponse::getDiffs, pageToken -> get(paramsForPage.apply(p, pageToken)));
  }
}
