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
import org.projectnessie.client.api.GetEntriesBuilder;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.EntriesResponse;
import org.projectnessie.model.EntriesResponse.Entry;

public abstract class BaseGetEntriesBuilder<PARAMS>
    extends BaseOnReferenceBuilder<GetEntriesBuilder> implements GetEntriesBuilder {

  private final BiFunction<PARAMS, String, PARAMS> paramsForPage;
  private String pageToken;

  protected Integer maxRecords;
  protected final List<ContentKey> keys = new ArrayList<>();
  protected ContentKey minKey;
  protected ContentKey maxKey;
  protected ContentKey prefixKey;
  protected String filter;
  protected boolean withContent;
  protected boolean forWrite;

  protected BaseGetEntriesBuilder(BiFunction<PARAMS, String, PARAMS> paramsForPage) {
    this.paramsForPage = paramsForPage;
  }

  @Override
  public GetEntriesBuilder maxRecords(int maxRecords) {
    this.maxRecords = maxRecords;
    return this;
  }

  @Override
  public GetEntriesBuilder pageToken(String pageToken) {
    this.pageToken = pageToken;
    return this;
  }

  @Override
  public GetEntriesBuilder key(ContentKey key) {
    this.keys.add(key);
    return this;
  }

  @Override
  public GetEntriesBuilder keys(Collection<ContentKey> keys) {
    this.keys.addAll(keys);
    return this;
  }

  @Override
  public GetEntriesBuilder minKey(ContentKey minKey) {
    this.minKey = minKey;
    return this;
  }

  @Override
  public GetEntriesBuilder maxKey(ContentKey maxKey) {
    this.maxKey = maxKey;
    return this;
  }

  @Override
  public GetEntriesBuilder prefixKey(ContentKey prefixKey) {
    this.prefixKey = prefixKey;
    return this;
  }

  @Override
  public GetEntriesBuilder filter(String filter) {
    this.filter = filter;
    return this;
  }

  @Override
  public GetEntriesBuilder withContent(boolean withContent) {
    this.withContent = withContent;
    return this;
  }

  protected abstract PARAMS params();

  protected abstract EntriesResponse get(PARAMS p) throws NessieNotFoundException;

  @Override
  public EntriesResponse get() throws NessieNotFoundException {
    return get(paramsForPage.apply(params(), pageToken));
  }

  @Override
  public Stream<Entry> stream() throws NessieNotFoundException {
    PARAMS p = params();
    return StreamingUtil.generateStream(
        EntriesResponse::getEntries, pageToken -> get(paramsForPage.apply(p, pageToken)));
  }
}
