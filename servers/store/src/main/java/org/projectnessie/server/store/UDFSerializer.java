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
package org.projectnessie.server.store;

import org.projectnessie.model.Content;
import org.projectnessie.model.UDF;
import org.projectnessie.server.store.proto.ObjectTypes;

public final class UDFSerializer extends BaseSerializer<UDF> {

  @Override
  public Content.Type contentType() {
    return Content.Type.UDF;
  }

  @Override
  public int payload() {
    return 5;
  }

  @SuppressWarnings("deprecation")
  @Override
  protected void toStoreOnRefState(UDF udf, ObjectTypes.Content.Builder builder) {
    ObjectTypes.UDF.Builder stateBuilder = ObjectTypes.UDF.newBuilder();

    String dialect = udf.getDialect();
    if (dialect != null) {
      stateBuilder.setDialect(udf.getDialect());
    }
    String sqlText = udf.getSqlText();
    if (sqlText != null) {
      stateBuilder.setSqlText(udf.getSqlText());
    }

    String metadata = udf.getMetadataLocation();
    if (metadata != null) {
      stateBuilder.setMetadataLocation(udf.getMetadataLocation());
    }
    String version = udf.getVersionId();
    if (version != null) {
      stateBuilder.setVersionId(version);
    }
    String signature = udf.getSignatureId();
    if (signature != null) {
      stateBuilder.setSignatureId(signature);
    }

    builder.setUdf(stateBuilder);
  }

  @Override
  protected UDF valueFromStore(ObjectTypes.Content content) {
    return valueFromStoreUDF(content);
  }
}
