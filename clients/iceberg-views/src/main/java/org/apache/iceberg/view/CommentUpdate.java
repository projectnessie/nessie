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
package org.apache.iceberg.view;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Update comments API implementation. */
public class CommentUpdate implements UpdateComment {
  private static final Logger LOG = LoggerFactory.getLogger(CommentUpdate.class);

  private final ViewOperations ops;
  private final ViewVersionMetadata base;
  private final Schema schema;
  private final Map<Integer, Types.NestedField> updates = Maps.newHashMap();

  public CommentUpdate(ViewOperations ops) {
    this.ops = ops;
    this.base = ops.current();
    this.schema = base.definition().schema();
  }

  @Override
  public UpdateComment updateColumnDoc(String name, String doc) {
    Types.NestedField field = schema.findField(name);
    Preconditions.checkArgument(field != null, "Cannot update missing column: %s", name);

    int fieldId = field.fieldId();
    updates.put(fieldId, required(fieldId, field.name(), field.type(), doc));
    return this;
  }

  /**
   * Apply the pending changes to the original schema and returns the result.
   *
   * <p>This does not result in a permanent update.
   *
   * @return the result Schema when all pending updates are applied
   */
  @Override
  public Schema apply() {
    return applyChanges(schema, updates);
  }

  @Override
  public void commit() {
    ViewDefinition baseDefinition = base.definition();
    ViewDefinition viewDefinition =
        ViewDefinition.of(
            baseDefinition.sql(),
            apply(),
            baseDefinition.sessionCatalog(),
            baseDefinition.sessionNamespace());
    // When metacat alters a column comment, it goes through this code path. Genie-id is not
    // available
    // in that case. When an engine (e.g. Spark) alters a column comment, it turns into a 'replace'
    // view
    // operation and goes through a different code path.
    Map<String, String> summaryProps = new HashMap<>();
    summaryProps.put(CommonViewConstants.OPERATION, DDLOperations.ALTER_COMMENT);
    summaryProps.put(CommonViewConstants.GENIE_ID, "N/A");
    summaryProps.put(CommonViewConstants.ENGINE_VERSION, "N/A");
    VersionSummary summary = new VersionSummary(summaryProps);
    BaseVersion version =
        new BaseVersion(
            base.currentVersionId() + 1,
            base.currentVersionId(),
            System.currentTimeMillis(),
            summary,
            viewDefinition);
    ViewVersionMetadata update =
        ViewVersionMetadata.newViewVersionMetadata(
            version, base.location(), viewDefinition, base, base.properties());
    ops.commit(base, update, new HashMap<>());
  }

  private static Schema applyChanges(Schema schema, Map<Integer, Types.NestedField> updates) {
    Types.StructType struct =
        TypeUtil.visit(schema, new ApplyChanges(updates)).asNestedType().asStructType();
    return new Schema(struct.fields());
  }

  private static class ApplyChanges extends TypeUtil.SchemaVisitor<Type> {
    private final Map<Integer, Types.NestedField> updates;

    private ApplyChanges(Map<Integer, Types.NestedField> updates) {
      this.updates = updates;
    }

    @Override
    public Type schema(Schema schema, Type structResult) {
      return structResult;
    }

    @Override
    public Type struct(Types.StructType struct, List<Type> fieldResults) {
      boolean hasChange = false;
      List<Types.NestedField> newFields = Lists.newArrayListWithExpectedSize(fieldResults.size());
      for (int i = 0; i < fieldResults.size(); i += 1) {
        Type resultType = fieldResults.get(i);
        if (resultType == null) {
          hasChange = true;
          continue;
        }

        Types.NestedField field = struct.fields().get(i);
        String name = field.name();
        String doc = field.doc();
        Types.NestedField update = updates.get(field.fieldId());
        if (update != null) {
          name = update.name();
          doc = update.doc();
        }

        if (!name.equals(field.name())
            || field.type() != resultType
            || !Objects.equals(doc, field.doc())) {
          hasChange = true;
          if (field.isOptional()) {
            newFields.add(optional(field.fieldId(), name, resultType, doc));
          } else {
            newFields.add(required(field.fieldId(), name, resultType, doc));
          }
        } else {
          newFields.add(field);
        }
      }

      if (hasChange) {
        // TODO: What happens if there are no fields left?
        return Types.StructType.of(newFields);
      }

      return struct;
    }

    @Override
    public Type field(Types.NestedField field, Type fieldResult) {
      // handle updates
      Types.NestedField update = updates.get(field.fieldId());
      if (update != null && update.type() != field.type()) {
        // rename is handled in struct, but struct needs the correct type from the field result
        return update.type();
      }
      return fieldResult;
    }

    @Override
    public Type list(Types.ListType list, Type result) {
      // use field to apply updates
      Type elementResult = field(list.fields().get(0), result);
      if (elementResult == null) {
        throw new IllegalArgumentException("Cannot delete element type from list: " + list);
      }

      if (list.elementType() == elementResult) {
        return list;
      }

      if (list.isElementOptional()) {
        return Types.ListType.ofOptional(list.elementId(), elementResult);
      } else {
        return Types.ListType.ofRequired(list.elementId(), elementResult);
      }
    }

    @Override
    public Type map(Types.MapType map, Type keyResult, Type valResult) {
      // if any updates are intended for the key, throw an exception
      int keyId = map.fields().get(0).fieldId();
      if (updates.containsKey(keyId)) {
        throw new IllegalArgumentException("Cannot update map keys: " + map);
      } else if (!map.keyType().equals(keyResult)) {
        throw new IllegalArgumentException("Cannot alter map keys: " + map);
      }

      // use field to apply updates to the value
      Type valueResult = field(map.fields().get(1), valResult);
      if (valueResult == null) {
        throw new IllegalArgumentException("Cannot delete value type from map: " + map);
      }

      if (map.valueType() == valueResult) {
        return map;
      }

      if (map.isValueOptional()) {
        return Types.MapType.ofOptional(map.keyId(), map.valueId(), map.keyType(), valueResult);
      } else {
        return Types.MapType.ofRequired(map.keyId(), map.valueId(), map.keyType(), valueResult);
      }
    }

    @Override
    public Type primitive(Type.PrimitiveType primitive) {
      return primitive;
    }
  }
}
