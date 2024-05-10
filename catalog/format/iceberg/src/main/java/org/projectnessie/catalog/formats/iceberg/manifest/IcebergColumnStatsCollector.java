/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.catalog.formats.iceberg.manifest;

import static java.util.Objects.requireNonNull;
import static org.projectnessie.catalog.formats.iceberg.meta.IcebergPartitionFieldSummary.icebergPartitionFieldSummary;
import static org.projectnessie.catalog.formats.iceberg.types.IcebergStructType.FIELD_ID_PROP;

import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergPartitionField;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergPartitionFieldSummary;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergPartitionSpec;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergSchema;
import org.projectnessie.catalog.formats.iceberg.types.IcebergType;
import org.projectnessie.catalog.model.manifest.BooleanArray;

public class IcebergColumnStatsCollector {
  private final IcebergPartitionSpec partitionSpec;
  private int deletedDataFilesCount;
  private long deletedRowsCount;
  private int addedDataFilesCount;
  private long addedRowsCount;
  private int existingDataFilesCount;
  private long existingRowsCount;
  private final PartitionFieldSummaryBuilder[] summary;

  public IcebergColumnStatsCollector(IcebergSchema schema, IcebergPartitionSpec partitionSpec) {
    this.partitionSpec = partitionSpec;
    List<IcebergPartitionField> partitionFields = partitionSpec.fields();
    summary = new PartitionFieldSummaryBuilder[partitionFields.size()];
    for (int i = 0; i < summary.length; i++) {
      IcebergPartitionField field = partitionFields.get(i);
      summary[i] = new PartitionFieldSummaryBuilder(field.type(schema), field.fieldId());
    }
  }

  public void addManifestEntry(IcebergManifestEntry entry) {
    GenericData.Record partition = entry.dataFile().partition();

    List<Schema.Field> partitionSchemeFields = partition.getSchema().getFields();
    List<IcebergPartitionField> fields = partitionSpec.fields();
    for (int i = 0; i < fields.size(); i++) {
      IcebergPartitionField partitionField = fields.get(i);

      int pos = -1;
      for (Schema.Field field : partitionSchemeFields) {
        String fieldId = field.getProp(FIELD_ID_PROP);
        if (fieldId != null) {
          if (partitionField.fieldId() == Integer.parseInt(fieldId)) {
            pos = field.pos();
            break;
          }
        }
      }
      Object value = pos == -1 ? partition.get(partitionField.name()) : partition.get(pos);
      PartitionFieldSummaryBuilder summaryBuilder = summary[i];
      if (value == null) {
        summaryBuilder.nullValueCount++;
      } else {
        summaryBuilder.valueCount++;
        Object lowerBound = summaryBuilder.lowerBound;
        if (lowerBound == null) {
          summaryBuilder.lowerBound = value;
        } else if (summaryBuilder.type.compare(lowerBound, value) < 0) {
          summaryBuilder.lowerBound = value;
        }
        Object upperBound = summaryBuilder.upperBound;
        if (upperBound == null) {
          summaryBuilder.upperBound = value;
        } else if (summaryBuilder.type.compare(lowerBound, value) > 0) {
          summaryBuilder.lowerBound = value;
        }
        // TODO partition-summary NaN
      }
    }

    long recordCount = requireNonNull(entry.dataFile()).recordCount();
    switch (entry.status()) {
      case ADDED:
        addedDataFilesCount++;
        addedRowsCount += recordCount;
        break;
      case DELETED:
        deletedDataFilesCount++;
        deletedRowsCount += recordCount;
        break;
      case EXISTING:
        existingDataFilesCount++;
        existingRowsCount += recordCount;
        break;
      default:
        throw new IllegalArgumentException("Unknown manifest-entry status " + entry.status());
    }
  }

  public void addToManifestFileBuilder(IcebergManifestFile.Builder manifestFile) {
    for (PartitionFieldSummaryBuilder partitionFieldSummaryBuilder : summary) {
      manifestFile.addPartitions(partitionFieldSummaryBuilder.asSummary());
    }

    manifestFile
        .deletedFilesCount(deletedDataFilesCount)
        .deletedRowsCount(deletedRowsCount)
        .addedRowsCount(addedRowsCount)
        .addedFilesCount(addedDataFilesCount)
        .existingRowsCount(existingRowsCount)
        .existingFilesCount(existingDataFilesCount);
  }

  private static final class PartitionFieldSummaryBuilder {
    final IcebergType type;
    final int fieldId;
    long nullValueCount;
    long nanValueCount;
    long valueCount;
    Object lowerBound;
    Object upperBound;

    PartitionFieldSummaryBuilder(IcebergType type, int fieldId) {
      this.type = type;
      this.fieldId = fieldId;
    }

    IcebergPartitionFieldSummary asSummary() {
      // TODO serialize lower-bound and upper-bound to byte-buffer
      // TODO "If -0.0 is a value of the partition field, the lower_bound must not be +0.0, and if
      //  +0.0 is a value of the partition field, the upper_bound must not be -0.0."
      // TODO see https://iceberg.apache.org/spec/#binary-single-value-serialization
      return icebergPartitionFieldSummary(
          nullValueCount > 0, lowerBoundBytes(), upperBoundBytes(), containsNan());
    }

    private Boolean containsNan() {
      return nanValueCount > 0 ? true : null;
    }

    public void intoNessieFieldSummaries(
        int i,
        int[] fieldIds,
        BooleanArray containsNan,
        BooleanArray containsNull,
        long[] nanValueCount,
        long[] nullValueCount,
        byte[][] upperBound,
        byte[][] lowerBound,
        long[] valueCount) {
      fieldIds[i] = this.fieldId;
      containsNan.set(i, this.nanValueCount > 0 ? Boolean.TRUE : null);
      containsNull.set(i, this.nullValueCount > 0);
      nanValueCount[i] = this.nanValueCount;
      nullValueCount[i] = this.nullValueCount;
      upperBound[i] = this.upperBoundBytes();
      lowerBound[i] = this.lowerBoundBytes();
      valueCount[i] = this.valueCount;
    }

    static byte encodeNullableBoolean(Boolean b) {
      if (b == null) {
        return 0;
      }
      return (byte) (b ? 1 : 2);
    }

    private byte[] lowerBoundBytes() {
      Object lower = lowerBound;
      return lower != null ? type.serializeSingleValue(lower) : null;
    }

    private byte[] upperBoundBytes() {
      Object upper = upperBound;
      return upper != null ? type.serializeSingleValue(upper) : null;
    }
  }
}
