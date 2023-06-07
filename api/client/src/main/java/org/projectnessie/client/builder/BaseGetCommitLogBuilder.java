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

import java.util.function.BiFunction;
import java.util.stream.Stream;
import org.projectnessie.client.api.GetCommitLogBuilder;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.FetchOption;
import org.projectnessie.model.LogResponse;
import org.projectnessie.model.LogResponse.LogEntry;

public abstract class BaseGetCommitLogBuilder<PARAMS>
    extends BaseOnReferenceBuilder<GetCommitLogBuilder> implements GetCommitLogBuilder {

  private final BiFunction<PARAMS, String, PARAMS> paramsForPage;
  private String pageToken;

  protected Integer maxRecords;
  protected FetchOption fetchOption;
  protected String filter;
  protected String untilHash;

  protected BaseGetCommitLogBuilder(BiFunction<PARAMS, String, PARAMS> paramsForPage) {
    this.paramsForPage = paramsForPage;
  }

  @Override
  public GetCommitLogBuilder fetch(FetchOption fetchOption) {
    this.fetchOption = fetchOption;
    return this;
  }

  @Override
  public GetCommitLogBuilder untilHash(String untilHash) {
    this.untilHash = untilHash;
    return this;
  }

  @Override
  public GetCommitLogBuilder maxRecords(int maxRecords) {
    this.maxRecords = maxRecords;
    return this;
  }

  @Override
  public GetCommitLogBuilder pageToken(String pageToken) {
    this.pageToken = pageToken;
    return this;
  }

  @Override
  public GetCommitLogBuilder filter(String filter) {
    this.filter = filter;
    return this;
  }

  protected abstract PARAMS params();

  @Override
  public LogResponse get() throws NessieNotFoundException {
    return get(paramsForPage.apply(params(), pageToken));
  }

  protected abstract LogResponse get(PARAMS p) throws NessieNotFoundException;

  @Override
  public Stream<LogEntry> stream() throws NessieNotFoundException {
    PARAMS p = params();
    return StreamingUtil.generateStream(
        LogResponse::getLogEntries, pageToken -> get(paramsForPage.apply(p, pageToken)));
  }
}
