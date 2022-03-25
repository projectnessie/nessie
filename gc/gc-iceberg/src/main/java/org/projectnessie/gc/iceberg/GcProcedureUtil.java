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

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.spark.procedures.BaseGcProcedure;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.analysis.NoSuchProcedureException;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.unsafe.types.UTF8String;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.reflect.ClassTag;

final class GcProcedureUtil {

  private GcProcedureUtil() {}

  static final String NAMESPACE = "nessie_gc";
  static final String[] NAMESPACE_ARRAY = {NAMESPACE};

  static boolean isGcNamespace(Identifier identifier) {
    return Arrays.equals(NAMESPACE_ARRAY, identifier.namespace());
  }

  static BaseGcProcedure loadGcProcedure(Identifier procedureIdentifier, TableCatalog catalog)
      throws NoSuchProcedureException {
    switch (procedureIdentifier.name()) {
      case DummyProcedure.PROCEDURE_NAME:
        return new DummyProcedure(catalog);
      case IdentifyExpiredSnapshotsProcedure.PROCEDURE_NAME:
        return new IdentifyExpiredSnapshotsProcedure(catalog);
      case ExpireSnapshotsProcedure.PROCEDURE_NAME:
        return new ExpireSnapshotsProcedure(catalog);
      default:
        throw new NoSuchProcedureException(procedureIdentifier);
    }
  }

  static InternalRow internalRow(Object... columns) {
    Seq<Object> seq =
        JavaConverters.collectionAsScalaIterable(
                Arrays.stream(columns)
                    .map(GcProcedureUtil::toSparkObject)
                    .collect(Collectors.toList()))
            .toSeq();
    return InternalRow.fromSeq(seq);
  }

  private static Object toSparkObject(Object object) {
    if (object instanceof String) {
      return UTF8String.fromString((String) object);
    }
    if (object instanceof List) {
      List<?> converted =
          ((List<?>) object)
              .stream().map(GcProcedureUtil::toSparkObject).collect(Collectors.toList());
      return ArrayData.toArrayData(
          JavaConverters.collectionAsScalaIterable(converted).toArray(ClassTag.Any()));
    }
    return object;
  }
}
