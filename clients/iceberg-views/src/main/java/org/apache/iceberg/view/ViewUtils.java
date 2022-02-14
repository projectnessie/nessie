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

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.iceberg.view.CommonViewConstants.OPERATION;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;

/** Utility methods for operating on common views. */
public class ViewUtils {
  /**
   * Method picks and returns the 'summary' properties from the map of table properties. Summary
   * properties are recorded in the 'sumnmary' portion of 'Version' in metadata json file.
   *
   * @param operation The view operation that results in alteration of the view
   * @param properties Map of all table properties
   * @param prevProperties Properties previously set
   * @return A map of summary properties to be recorded in the metadata json file. These are all
   *     previously set properties overlayed with the new properties.
   */
  public static Map<String, String> getSummaryProperties(
      String operation, Map<String, String> properties, Map<String, String> prevProperties) {
    HashMap<String, String> props = new HashMap<>();
    String val;
    for (CommonViewConstants.ViewVersionSummaryConstants key :
        CommonViewConstants.ViewVersionSummaryConstants.values()) {
      val = properties.get(key.name());
      if (val != null) {
        props.put(String.valueOf(key), val);
      } else if (prevProperties != null) {
        val = prevProperties.get(key.name());
        if (val != null) {
          props.put(String.valueOf(key), val);
        }
      }
    }
    props.put(OPERATION, operation);
    return props;
  }

  /**
   * The method returns the properties to be recorded as table properties by metacat.
   *
   * @param properties Map of all table properties
   * @return Map of properties to be recorded as table properties by metacat Metacat applies these
   *     properties as an update in case the properties already exist e.g. in case of a 'replace
   *     view'
   */
  public static Map<String, String> getMetacatProperties(Map<String, String> properties) {
    HashMap<String, String> props = new HashMap<>();
    String val;
    for (EngineProperties key : EngineProperties.values()) {
      val = properties.get(key.name());
      if (val != null) {
        props.put(String.valueOf(key), val);
      }
    }
    return props;
  }

  /**
   * Method picks and returns common view specific properties from the map of table properties.
   * These properties are recorded in the 'properties' section of the view version metadata file.
   * Any properties that were previously set and are not being overridden are persisted.
   *
   * @param properties Map of all table properties
   * @param prevProperties Properties that were previously set
   * @param summaryProperties 'sumnmary' portion of 'Version' in metadata json file.
   * @param metacatProperties properties to be recorded as table properties by metacat
   * @return A map of properties to be recorded in the metadata json file.
   */
  public static Map<String, String> getViewVersionMetadataProperties(
      Map<String, String> properties,
      Map<String, String> prevProperties,
      Map<String, String> summaryProperties,
      Map<String, String> metacatProperties) {
    HashMap<String, String> props = new HashMap<>(prevProperties);
    props.putAll(properties);
    props.keySet().removeAll(summaryProperties.keySet());
    props.keySet().removeAll(metacatProperties.keySet());
    return props;
  }

  /**
   * The method prepares the arguments to perform the commit and then proceeds to commit.
   *
   * @param operation View operation causing the commit
   * @param properties Table attributes and properties sent by the engine
   * @param versionId Current version id.
   * @param parentId Version id of the parent version.
   * @param definition View definition
   * @param location Location of view metadata
   * @param ops View operations object needed to perform the commit
   * @param prevViewVersionMetadata Previous view version metadata
   */
  public static void doCommit(
      String operation,
      Map<String, String> properties,
      int versionId,
      int parentId,
      ViewDefinition definition,
      String location,
      ViewOperations ops,
      ViewVersionMetadata prevViewVersionMetadata) {
    Map<String, String> prevSummaryProps;
    Map<String, String> prevViewVersionMetadataProps;

    if (prevViewVersionMetadata != null) {
      prevSummaryProps = prevViewVersionMetadata.currentVersion().summary().properties();
      prevViewVersionMetadataProps = prevViewVersionMetadata.properties();
    } else {
      prevSummaryProps = new HashMap<>();
      prevViewVersionMetadataProps = new HashMap<>();
    }

    // The input set of table properties need to be classified in three sets of properties:
    // 1) Summary properties: these are recorded with a particular version of the view (Defined in
    // CommonViewConstants.java)
    // 2) Metacat properties: these are recorded by the metacat as table properties. These are
    // object level properties set by
    //    engines. (Defined in EngineProperties.java)
    // 3) View version metadata properties: these are not versioned. These are all the other table
    // properties that do not
    //    belong in 1) or 2) above.
    Map<String, String> summaryProps =
        ViewUtils.getSummaryProperties(operation, properties, prevSummaryProps);
    VersionSummary summary = new VersionSummary(summaryProps);

    Map<String, String> metacatProps = ViewUtils.getMetacatProperties(properties);

    Map<String, String> viewVersionMetadataProperties =
        ViewUtils.getViewVersionMetadataProperties(
            properties, prevViewVersionMetadataProps, summaryProps, metacatProps);

    // Retain the column comments from previous version of the view if the new version does not have
    // column comments
    ViewDefinition definitionWithComments = definition;
    if (prevViewVersionMetadata != null) {
      definitionWithComments =
          retainColumnComments(definition, prevViewVersionMetadata.definition());
    }

    BaseVersion version =
        new BaseVersion(
            versionId, parentId, System.currentTimeMillis(), summary, definitionWithComments);
    ViewVersionMetadata viewVersionMetadata;
    if (prevViewVersionMetadata == null) {
      viewVersionMetadata =
          ViewVersionMetadata.newViewVersionMetadata(
              version, location, definitionWithComments, viewVersionMetadataProperties);
    } else {
      viewVersionMetadata =
          ViewVersionMetadata.newViewVersionMetadata(
              version,
              location,
              definitionWithComments,
              prevViewVersionMetadata,
              viewVersionMetadataProperties);
    }

    ops.commit(prevViewVersionMetadata, viewVersionMetadata, metacatProps);
  }

  /**
   * The method ensures that when a view is getting REPLACEd and a new column comment has not been
   * specified (indicated by 'doc' field being null), column comment from the previous version of
   * the view is retained.
   *
   * @param newDef new view definition, definition specified by REPLACE
   * @param oldDef current view definition
   * @return new view definition enhanced with column comments from current view definition where
   *     applicable.
   */
  public static ViewDefinition retainColumnComments(ViewDefinition newDef, ViewDefinition oldDef) {
    List<Types.NestedField> newCols = new ArrayList<>();
    for (Types.NestedField col : newDef.schema().columns()) {
      if (col.doc() == null) {
        Types.NestedField oldCol = oldDef.schema().caseInsensitiveFindField(col.name());
        if (oldCol != null) {
          Types.NestedField newCol =
              Types.NestedField.of(
                  col.fieldId(), col.isOptional(), col.name(), col.type(), oldCol.doc());
          newCols.add(newCol);
        } else {
          newCols.add(col);
        }
      } else {
        newCols.add(col);
      }
    }
    Schema enhancedSchema = new Schema(newCols);
    return new BaseViewDefinition(
        newDef.sql(), enhancedSchema, newDef.sessionCatalog(), newDef.sessionNamespace());
  }

  public static void validateTableIdentifier(TableIdentifier viewIdentifier) {
    checkArgument(viewIdentifier.hasNamespace(), "viewIdentifier should have namespace");
    checkArgument(
        viewIdentifier.namespace().levels().length == 2, "namespace should have catalog.schema");
  }

  public static TableIdentifier toCatalogTableIdentifier(String tableIdentifier) {
    List<String> namespace = Lists.newArrayList();
    Iterable<String> parts = Splitter.on(".").split(tableIdentifier);

    String lastPart = "";
    for (String part : parts) {
      if (!lastPart.isEmpty()) {
        namespace.add(lastPart);
      }
      lastPart = part;
    }

    Preconditions.checkState(
        namespace.size() == 2, "Catalog and schema are expected in the namespace.");

    return TableIdentifier.of(Namespace.of(namespace.toArray(new String[0])), lastPart);
  }
}
