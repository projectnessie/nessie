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
package org.projectnessie.gc.iceberg;

import org.apache.iceberg.spark.procedures.BaseGcProcedure;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.iceberg.catalog.ProcedureParameter;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/** Dummy procedure that accepts an input string and returns it. */
public class DummyProcedure extends BaseGcProcedure {

  public static final String PROCEDURE_NAME = "dummy";

  private static final ProcedureParameter[] PARAMETERS =
      new ProcedureParameter[] {ProcedureParameter.required("input_string", DataTypes.StringType)};

  public static final String OUTPUT_RUN_ID = "output_string";

  private static final StructType OUTPUT_TYPE =
      new StructType(
          new StructField[] {
            new StructField(OUTPUT_RUN_ID, DataTypes.StringType, true, Metadata.empty())
          });

  private InternalRow resultRow(String runId) {
    return GcProcedureUtil.internalRow(runId);
  }

  public DummyProcedure(TableCatalog currentCatalog) {
    super(currentCatalog);
  }

  @Override
  public ProcedureParameter[] parameters() {
    return PARAMETERS;
  }

  @Override
  public StructType outputType() {
    return OUTPUT_TYPE;
  }

  @Override
  public String description() {
    return "Dummy procedure that accepts an input string and returns it.";
  }

  @Override
  public InternalRow[] call(InternalRow internalRow) {
    String inputString = internalRow.getString(0);
    return new InternalRow[] {resultRow(inputString)};
  }
}
