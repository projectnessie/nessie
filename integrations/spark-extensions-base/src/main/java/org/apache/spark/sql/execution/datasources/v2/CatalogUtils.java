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

import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URLDecoder;
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.CachingCatalog;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.spark.source.HasIcebergCatalog;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.projectnessie.api.v2.params.ParsedReference;
import org.projectnessie.client.NessieClientBuilder;
import org.projectnessie.client.NessieConfigConstants;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.client.config.NessieClientConfigSource;

public final class CatalogUtils {

  private CatalogUtils() {}

  public static CatalogBridge buildBridge(CatalogPlugin currentCatalog, String catalog) {

    @SuppressWarnings("resource")
    SparkSession sparkSession = SparkSession.active();
    SparkContext sparkContext = sparkSession.sparkContext();

    if (!(currentCatalog instanceof HasIcebergCatalog)) {
      currentCatalog = sparkSession.sessionState().catalogManager().catalog(catalog);
    }
    if (!(currentCatalog instanceof HasIcebergCatalog)) {
      throw new IllegalArgumentException(
          "The command works only when the catalog is a NessieCatalog or a RESTCatalog using the Nessie Catalog Server, "
              + "but "
              + catalog
              + " does not expose an Iceberg catalog. "
              + "Either set the catalog via USE <catalog_name> or provide the catalog during execution: <command> IN <catalog_name>.");
    }

    Catalog icebergCatalog = ((HasIcebergCatalog) currentCatalog).icebergCatalog();
    icebergCatalog = unwrapCachingCatalog(icebergCatalog);

    switch (icebergCatalog.getClass().getSimpleName()) {
      case "NessieCatalog":
        return new NessieCatalogBridge(sparkContext, currentCatalog, catalog);
      case "RESTCatalog":
        return new RestCatalogBridge(sparkContext, currentCatalog, catalog, icebergCatalog);
      default:
        throw new IllegalArgumentException(
            "The command works only when the catalog is a NessieCatalog or a RESTCatalog using the Nessie Catalog Server, "
                + "but "
                + catalog
                + " is a "
                + icebergCatalog.getClass().getName()
                + ". "
                + "Either set the catalog via USE <catalog_name> or provide the catalog during execution: <command> IN <catalog_name>.");
    }
  }

  static NessieApiV1 buildApi(NessieClientConfigSource nessieClientConfigMapper) {
    NessieClientBuilder nessieClientBuilder =
        NessieClientBuilder.createClientBuilderFromSystemSettings(nessieClientConfigMapper);
    String clientApiVer = nessieClientConfigMapper.getValue("nessie.client-api-version");
    if (clientApiVer == null) {
      clientApiVer = "1";
    }
    switch (clientApiVer) {
      case "1":
        return nessieClientBuilder.build(NessieApiV1.class);
      case "2":
        return nessieClientBuilder.build(NessieApiV2.class);
      default:
        throw new IllegalArgumentException(
            String.format(
                "Unsupported client-api-version value: %s. Can only be 1 or 2", clientApiVer));
    }
  }

  static String refFromRestPrefix(String prefix) {
    prefix = decodePrefix(prefix);
    int idx = prefix.indexOf("|");
    return idx == -1 ? prefix : prefix.substring(0, idx);
  }

  static Optional<String> warehouseFromRestPrefix(String prefix) {
    prefix = decodePrefix(prefix);
    int idx = prefix.indexOf("|");
    return idx == -1 ? Optional.empty() : Optional.of(prefix.substring(idx + 1));
  }

  static ParsedReference referenceFromRestPrefix(String prefix) {
    prefix = decodePrefix(prefix);
    String ref = refFromRestPrefix(prefix);
    int i = ref.indexOf("@");
    return i == -1
        ? ParsedReference.parsedReference(ref, null, null)
        : ParsedReference.parsedReference(ref.substring(0, i), ref.substring(i + 1), null);
  }

  private static String decodePrefix(String prefix) {
    try {
      prefix = URLDecoder.decode(prefix, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
    return prefix;
  }

  static Map<String, String> propertiesFromCatalog(Catalog icebergCatalog) {
    try {
      // This is funny: all Iceberg catalogs have a function `Map<String, String> properties()`.
      // In `BaseMetastoreCatalog` it is protected, in `RESTCatalog`, which does not extend
      // `BaseMetastoreCatalog`, it is public.
      Method catalogProperties = icebergCatalog.getClass().getDeclaredMethod("properties");
      catalogProperties.setAccessible(true);

      // Call the `properties()` function to get the catalog's configuration.
      Object props = catalogProperties.invoke(icebergCatalog);

      @SuppressWarnings({"UnnecessaryLocalVariable", "unchecked"})
      Map<String, String> properties = (Map<String, String>) props;
      return properties;
    } catch (Exception e) {
      throw new IllegalStateException(
          "Iceberg catalog implementation "
              + icebergCatalog.getClass().getName()
              + " does not expose its properties");
    }
  }

  static Catalog unwrapCachingCatalog(Catalog icebergCatalog) {
    try {
      // If the Iceberg catalog is a `CachingCatalog`, get the "base" catalog from it.
      if (icebergCatalog instanceof CachingCatalog) {
        Field cachingCatalogCatalog = icebergCatalog.getClass().getDeclaredField("catalog");
        cachingCatalogCatalog.setAccessible(true);
        icebergCatalog = (Catalog) cachingCatalogCatalog.get(icebergCatalog);
      }

      return icebergCatalog;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /** Allow resolving a property via the environment. */
  static String resolveViaEnvironment(
      Map<String, String> properties, String property, String defaultValue) {
    String value = properties.get(property);
    if (value == null) {
      return defaultValue;
    }
    if (value.startsWith("env:")) {
      String env = System.getenv(value.substring("env:".length()));
      if (env == null) {
        return defaultValue;
      }
      return env;
    } else {
      return value;
    }
  }

  static String resolveOAuthScope(Map<String, String> catalogProperties) {
    String nessieScope =
        resolveViaEnvironment(
            catalogProperties, NessieConfigConstants.CONF_NESSIE_OAUTH2_CLIENT_SCOPES, null);
    if (nessieScope != null) {
      return nessieScope;
    } else {
      return resolveViaEnvironment(catalogProperties, "scope", "catalog");
    }
  }

  static Credential resolveCredential(Map<String, String> catalogProperties) {
    String nessieClientId =
        resolveViaEnvironment(
            catalogProperties, NessieConfigConstants.CONF_NESSIE_OAUTH2_CLIENT_ID, null);
    String nessieClientSecret =
        resolveViaEnvironment(
            catalogProperties, NessieConfigConstants.CONF_NESSIE_OAUTH2_CLIENT_SECRET, null);

    Credential credentialFromIceberg =
        parseIcebergCredential(resolveViaEnvironment(catalogProperties, "credential", null));

    return new Credential(
        nessieClientId != null ? nessieClientId : credentialFromIceberg.clientId,
        nessieClientSecret != null ? nessieClientSecret : credentialFromIceberg.secret);
  }

  static Credential parseIcebergCredential(String credential) {
    // See o.a.i.rest.auth.OAuth2Util.parseCredential
    if (credential == null) {
      return new Credential(null, null);
    }
    int colon = credential.indexOf(':');
    if (colon == -1) {
      return new Credential(null, credential);
    } else {
      return new Credential(credential.substring(0, colon), credential.substring(colon + 1));
    }
  }

  static void reinitializeCatalog(Catalog catalog, Map<String, String> properties) {
    if (catalog instanceof AutoCloseable) {
      try {
        ((AutoCloseable) catalog).close();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    String name = catalog.name();
    catalog.initialize(name, properties);
  }

  static final class Credential {
    final String clientId;
    final String secret;

    Credential(String clientId, String secret) {
      this.clientId = clientId;
      this.secret = secret;
    }
  }
}
