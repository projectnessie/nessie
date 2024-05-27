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

import static java.lang.Boolean.parseBoolean;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.iceberg.catalog.Catalog;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.projectnessie.api.v2.params.ParsedReference;
import org.projectnessie.client.NessieConfigConstants;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.client.config.NessieClientConfigSource;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.error.NessieReferenceNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.Reference;
import org.projectnessie.model.Tag;
import scala.Tuple2;

final class RestCatalogBridge implements CatalogBridge {
  private final SparkContext sparkContext;
  private final NessieApiV1 api;
  private final CatalogPlugin currentCatalog;
  private final String catalogName;
  private final String confPrefix;
  private final Catalog icebergCatalog;

  RestCatalogBridge(
      SparkContext sparkContext,
      CatalogPlugin currentCatalog,
      String catalogName,
      Catalog icebergCatalog) {
    this.sparkContext = sparkContext;
    this.currentCatalog = currentCatalog;
    this.catalogName = catalogName;
    this.confPrefix = "spark.sql.catalog." + catalogName + ".";
    this.icebergCatalog = icebergCatalog;

    Map<String, String> catalogProperties = CatalogUtils.propertiesFromCatalog(icebergCatalog);

    if (!parseBoolean(catalogProperties.get("nessie.is-nessie-catalog"))) {
      throw new IllegalArgumentException(
          "The command works only when the catalog is a NessieCatalog or a RESTCatalog using the Nessie Catalog Server"
              + ", but the referenced REST endpoint is not a Nessie Catalog Server. "
              + "Either set the catalog via USE <catalog_name> or provide the catalog during execution: <command> IN <catalog_name>.");
    }

    // See o.a.i.rest.auth.OAuth2Properties.CREDENTIAL
    CatalogUtils.Credential credential = CatalogUtils.resolveCredential(catalogProperties);

    Function<String, String> defaultConfigValue =
        x ->
            catalogProperties.containsKey(x)
                ? catalogProperties.get(x)
                : catalogProperties.get(x.replace("nessie.", ""));

    NessieClientConfigSource configSource =
        x -> {
          switch (x) {
            case NessieConfigConstants.CONF_NESSIE_URI:
              // Use the Nessie Core REST API URL provided by Nessie Catalog Server. The Nessie
              // Catalog
              // Server provides a _base_ URI without the `v1` or `v2` suffixes. We can safely
              // assume
              // that `nessie.core-base-uri` contains a `/` terminated URI.
              return catalogProperties.get("nessie.core-base-uri") + "v2";
            case "nessie.client-api-version":
              return "2";
            case NessieConfigConstants.CONF_NESSIE_OAUTH2_CLIENT_ID:
              return credential.clientId;
            case NessieConfigConstants.CONF_NESSIE_OAUTH2_CLIENT_SECRET:
              // See o.a.i.rest.auth.OAuth2Properties.CREDENTIAL
              return credential.secret;
            case NessieConfigConstants.CONF_NESSIE_OAUTH2_CLIENT_SCOPES:
              // Same default scope as the Iceberg REST Client uses in
              // o.a.i.rest.RESTSessionCatalog.initialize
              // See o.a.i.rest.auth.OAuth2Util.SCOPE
              return CatalogUtils.resolveOAuthScope(catalogProperties);
            case NessieConfigConstants.CONF_NESSIE_OAUTH2_AUTH_ENDPOINT:
              return catalogProperties.get("oauth2-server-uri");
            case NessieConfigConstants.CONF_NESSIE_AUTH_TOKEN:
              return catalogProperties.get("token");
            case NessieConfigConstants.CONF_NESSIE_AUTH_TYPE:
              if (catalogProperties.containsKey("token")) {
                return "BEARER";
              }
              return defaultConfigValue.apply(x);
            default:
              return defaultConfigValue.apply(x);
          }
        };

    this.api = CatalogUtils.buildApi(configSource);
  }

  @Override
  public Reference getCurrentRef() throws NessieReferenceNotFoundException {
    Map<String, String> catalogProperties = CatalogUtils.propertiesFromCatalog(icebergCatalog);
    String prefix = catalogProperties.get("prefix");
    ParsedReference parsed = CatalogUtils.referenceFromRestPrefix(prefix);

    String refHash = parsed.hashWithRelativeSpec();

    try {
      Reference ref = api.getReference().refName(parsed.name()).get();
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
              + parsed.name()
              + " configured in spark configuration for catalog '"
              + catalogName
              + "'.",
          e);
    }
  }

  @Override
  public void setCurrentRefForSpark(Reference ref, boolean configureRefAtHash) {
    Map<String, String> catalogProperties = CatalogUtils.propertiesFromCatalog(icebergCatalog);

    SparkConf sparkConf = sparkContext.conf();

    String warehouseSuffix =
        CatalogUtils.warehouseFromRestPrefix(catalogProperties.get("prefix")).orElse("");

    String refName = encode(ref.getName());

    String uri = catalogProperties.get("nessie.iceberg-base-uri") + refName;

    // we only configure ref.hash if we're reading data
    String prefix =
        (configureRefAtHash ? refName + "@" + ref.getHash() : refName) + warehouseSuffix;

    sparkConf.set(confPrefix + "uri", uri);
    sparkConf.set(confPrefix + "prefix", prefix);

    Map<String, String> catalogConf =
        Arrays.stream(sparkConf.getAllWithPrefix(confPrefix))
            .collect(Collectors.toMap(Tuple2::_1, Tuple2::_2));

    if (icebergCatalog instanceof AutoCloseable) {
      try {
        ((AutoCloseable) icebergCatalog).close();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    currentCatalog.initialize(catalogName, new CaseInsensitiveStringMap(catalogConf));
  }

  private static String encode(String s) {
    try {
      return URLEncoder.encode(s, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
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
