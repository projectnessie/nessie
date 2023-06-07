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
import org.projectnessie.client.api.GetRefLogBuilder;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.RefLogResponse;
import org.projectnessie.model.RefLogResponse.RefLogResponseEntry;

@Deprecated
public abstract class BaseGetRefLogBuilder<PARAMS> implements GetRefLogBuilder {

  private final BiFunction<PARAMS, String, PARAMS> paramsForPage;
  private String pageToken;

  protected String untilHash;
  protected String fromHash;
  protected String filter;
  protected Integer maxRecords;

  protected BaseGetRefLogBuilder(BiFunction<PARAMS, String, PARAMS> paramsForPage) {
    this.paramsForPage = paramsForPage;
  }

  @Override
  public GetRefLogBuilder untilHash(String untilHash) {
    this.untilHash = untilHash;
    return this;
  }

  @Override
  public GetRefLogBuilder fromHash(String fromHash) {
    this.fromHash = fromHash;
    return this;
  }

  @Override
  public GetRefLogBuilder filter(String filter) {
    this.filter = filter;
    return this;
  }

  @Override
  public GetRefLogBuilder maxRecords(int maxRecords) {
    this.maxRecords = maxRecords;
    return this;
  }

  @Override
  public GetRefLogBuilder pageToken(String pageToken) {
    this.pageToken = pageToken;
    return this;
  }

  protected abstract PARAMS params();

  protected abstract RefLogResponse get(PARAMS p) throws NessieNotFoundException;

  @Override
  public RefLogResponse get() throws NessieNotFoundException {
    return get(paramsForPage.apply(params(), pageToken));
  }

  @Override
  public Stream<RefLogResponseEntry> stream() throws NessieNotFoundException {
    PARAMS p = params();
    return StreamingUtil.generateStream(
        RefLogResponse::getLogEntries, pageToken -> get(paramsForPage.apply(p, pageToken)));
  }
}
