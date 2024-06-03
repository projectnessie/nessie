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
package org.apache.spark.sql.execution.datasources.v2;

import java.util.Arrays;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.error.NessieReferenceNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.Reference;
import org.projectnessie.model.Tag;
import scala.Tuple2;

final class NessieCatalogBridge implements CatalogBridge {
  private final SparkContext sparkContext;
  private final NessieApiV1 api;
  private final CatalogPlugin currentCatalog;
  private final String catalogName;
  private final String confPrefix;

  NessieCatalogBridge(SparkContext sparkContext, CatalogPlugin currentCatalog, String catalogName) {
    this.sparkContext = sparkContext;
    this.currentCatalog = currentCatalog;
    this.catalogName = catalogName;
    this.confPrefix = "spark.sql.catalog." + catalogName + ".";

    SparkConf sparkConf = sparkContext.conf();

    Map<String, String> catalogConf =
        Arrays.stream(sparkConf.getAllWithPrefix(confPrefix))
            .collect(Collectors.toMap(Tuple2::_1, Tuple2::_2));
    this.api = CatalogUtils.buildApi(x -> catalogConf.get(x.replace("nessie.", "")));
  }

  @Override
  public Reference getCurrentRef() throws NessieReferenceNotFoundException {
    SparkConf activeConf = sparkContext.conf();

    String refName = activeConf.get(confPrefix + "ref");
    String refHash;
    try {
      refHash = activeConf.get(confPrefix + "ref.hash");
    } catch (NoSuchElementException e) {
      refHash = null;
    }

    try {
      Reference ref = api.getReference().refName(refName).get();
      if (refHash != null) {
        if (ref.getType() == Reference.ReferenceType.BRANCH) {
          ref = Branch.of(ref.getName(), refHash);
        } else {
          ref = Tag.of(ref.getName(), refHash);
        }
      }
      return ref;
    } catch (NessieNotFoundException e) {
      throw new NessieReferenceNotFoundException(
          "Could not find current reference "
              + refName
              + " configured in spark configuration for catalog '"
              + catalogName
              + "'.",
          e);
    }
  }

  @Override
  public void setCurrentRefForSpark(Reference ref, boolean configureRefAtHash) {
    SparkConf activeConf = sparkContext.conf();

    activeConf.set(confPrefix + "ref", ref.getName());
    if (configureRefAtHash) {
      // we only configure ref.hash if we're reading data
      activeConf.set(confPrefix + "ref.hash", ref.getHash());
    } else {
      // we need to clear it in case it was previously set
      activeConf.remove(confPrefix + "ref.hash");
    }

    Map<String, String> catalogConf =
        Arrays.stream(activeConf.getAllWithPrefix(confPrefix))
            .collect(Collectors.toMap(Tuple2::_1, Tuple2::_2));

    currentCatalog.initialize(catalogName, new CaseInsensitiveStringMap(catalogConf));
  }

  @Override
  public NessieApiV1 api() {
    return api;
  }

  @Override
  public CatalogPlugin currentCatalog() {
    return currentCatalog;
  }

  @Override
  public void close() {
    api.close();
  }
}
