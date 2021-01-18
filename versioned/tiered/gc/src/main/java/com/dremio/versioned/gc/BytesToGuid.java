/*
 * Copyright (C) 2020 Dremio
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
package com.dremio.versioned.gc;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

import com.dremio.nessie.versioned.store.Id;


/**
 * Spark function to convert binary ids into string guids.
 */
class BytesToGuid implements org.apache.spark.sql.api.java.UDF1<byte[], String> {

  private static final long serialVersionUID = -6415976956653691170L;

  @Override
  public String call(byte[] t1) throws Exception {
    return Id.of(t1).toString();
  }


  public static Column toString(Column col) {
    return functions.udf(new BytesToGuid(), DataTypes.StringType).apply(col);

  }
}
