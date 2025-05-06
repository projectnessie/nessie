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
package org.projectnessie.server.authz;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.projectnessie.server.authn.AuthenticationEnabledProfile.AUTH_CONFIG_OVERRIDES;
import static org.projectnessie.server.authn.AuthenticationEnabledProfile.SECURITY_CONFIG;
import static org.projectnessie.server.authz.MockedAuthorizer.AuthzCheck.authzCheck;
import static org.projectnessie.server.catalog.IcebergCatalogTestCommon.WAREHOUSE_NAME;
import static org.projectnessie.services.authz.ApiContext.apiContext;
import static org.projectnessie.services.authz.Check.CheckType.CREATE_ENTITY;
import static org.projectnessie.services.authz.Check.CheckType.DELETE_ENTITY;
import static org.projectnessie.services.authz.Check.CheckType.READ_ENTITY_VALUE;
import static org.projectnessie.services.authz.Check.CheckType.UPDATE_ENTITY;
import static org.projectnessie.services.authz.Check.canCommitChangeAgainstReference;
import static org.projectnessie.services.authz.Check.canReadContentKey;
import static org.projectnessie.services.authz.Check.canReadEntries;
import static org.projectnessie.services.authz.Check.canViewReference;
import static org.projectnessie.services.authz.Check.check;

import com.google.common.collect.ImmutableMap;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.projectnessie.client.auth.BasicAuthenticationProvider;
import org.projectnessie.error.NessieForbiddenException;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.Operation;
import org.projectnessie.objectstoragemock.HeapStorageBucket;
import org.projectnessie.quarkus.tests.profiles.BaseConfigProfile;
import org.projectnessie.server.BaseClientAuthTest;
import org.projectnessie.server.catalog.Catalogs;
import org.projectnessie.server.catalog.S3UnitTestProfiles.S3UnitTestProfile;
import org.projectnessie.services.authz.ApiContext;
import org.projectnessie.services.authz.AuthorizerType;
import org.projectnessie.services.authz.BatchAccessChecker;
import org.projectnessie.services.authz.Check;
import org.projectnessie.versioned.BranchName;

@SuppressWarnings("resource") // api() returns an AutoCloseable
@QuarkusTest
@TestProfile(TestAuthzMeta.Profile.class)
public class TestAuthzMeta extends BaseClientAuthTest {
  @Inject
  @AuthorizerType("MOCKED")
  MockedAuthorizer mockedAuthorizer;

  HeapStorageBucket heapStorageBucket;

  private static final Catalogs CATALOGS = new Catalogs();

  // Cannot use @ExtendWith(SoftAssertionsExtension.class) + @InjectSoftAssertions here, because
  // of Quarkus class loading issues. See https://github.com/quarkusio/quarkus/issues/19814
  protected final SoftAssertions soft = new SoftAssertions();

  protected RESTCatalog catalog(Map<String, String> catalogOptions) {
    return CATALOGS.getCatalog(catalogOptions);
  }

  @AfterAll
  static void closeRestCatalog() throws Exception {
    CATALOGS.close();
  }

  @AfterEach
  void cleanup() {
    // Cannot use @ExtendWith(SoftAssertionsExtension.class) + @InjectSoftAssertions here, because
    // of Quarkus class loading issues. See https://github.com/quarkusio/quarkus/issues/19814
    soft.assertAll();
  }

  @BeforeEach
  void beforeEach() {
    mockedAuthorizer.reset();
    heapStorageBucket.clear();
  }

  /**
   * Verifies that the expected authz checks are issued with the right {@link ApiContext} and
   * "actions".
   */
  @Test
  public void icebergApiTable() {
    var catalog =
        catalog(Map.of("header.Authorization", basicAuthorizationHeader("admin_user", "test123")));

    var apiContext = apiContext("Iceberg", 1);
    var branch = BranchName.of("main");

    var myNamespaceIceberg = Namespace.of("iceberg_table");
    var tableKey = ContentKey.of("iceberg_table", "table_foo");
    var tableIdentifier = TableIdentifier.of(myNamespaceIceberg, "table_foo");

    mockedAuthorizer.reset();
    catalog.createNamespace(myNamespaceIceberg);
    // no assertion, done in 'icebergApiNamespaces()'

    var schema =
        new Schema(
            required(3, "id", Types.IntegerType.get()),
            required(4, "data", Types.StringType.get()));
    var spec = PartitionSpec.builderFor(schema).bucket("id", 16).build();

    mockedAuthorizer.reset();
    var props = new HashMap<String, String>();
    catalog.createTable(tableIdentifier, schema, spec, "my://location", props);
    soft.assertThat(mockedAuthorizer.checksWithoutIdentifiedKey())
        .containsExactly(
            // 'IcebergApiV1ResourceBase.createEntityVerifyNotExists'
            authzCheck(
                apiContext,
                List.of(
                    canViewReference(branch),
                    canCommitChangeAgainstReference(branch),
                    check(READ_ENTITY_VALUE, branch, tableKey),
                    check(CREATE_ENTITY, branch, tableKey)),
                Map.of()),
            // 'CatalogServiceImpl.locationForEntity'
            authzCheck(
                apiContext,
                List.of(
                    canViewReference(branch),
                    check(READ_ENTITY_VALUE, branch, tableKey.getParent())),
                Map.of()),
            // 'CatalogServiceImpl.commit'
            authzCheck(
                apiContext,
                List.of(
                    canViewReference(branch),
                    canCommitChangeAgainstReference(branch),
                    check(READ_ENTITY_VALUE, branch, tableKey, Set.of("CATALOG_CREATE_ENTITY")),
                    check(CREATE_ENTITY, branch, tableKey, Set.of("CATALOG_CREATE_ENTITY"))),
                Map.of()),
            // actual 'commit'
            authzCheck(
                apiContext,
                List.of(
                    canViewReference(branch),
                    canCommitChangeAgainstReference(branch),
                    check(
                        CREATE_ENTITY,
                        branch,
                        tableKey,
                        Set.of(
                            "CATALOG_CREATE_ENTITY",
                            "META_ADD_SORT_ORDER",
                            "META_SET_DEFAULT_PARTITION_SPEC",
                            "META_SET_CURRENT_SCHEMA",
                            "META_UPGRADE_FORMAT_VERSION",
                            "META_SET_PROPERTIES",
                            "META_ASSIGN_UUID",
                            "META_SET_LOCATION",
                            "META_ADD_SCHEMA",
                            "META_SET_DEFAULT_SORT_ORDER",
                            "META_ADD_PARTITION_SPEC"))),
                Map.of()));
  }

  /**
   * Verifies that the expected authz checks are issued with the right {@link ApiContext} and
   * "actions".
   */
  @Test
  public void icebergApiNamespaces() {
    var catalog =
        catalog(Map.of("header.Authorization", basicAuthorizationHeader("admin_user", "test123")));

    var myNamespace = ContentKey.of("iceberg_namespaces");
    var myNamespaceIceberg = Namespace.of("iceberg_namespaces");
    var myNamespaceInner = ContentKey.of("iceberg_namespaces", "inner");
    var myNamespaceIcebergInner = Namespace.of("iceberg_namespaces", "inner");

    var apiContext = apiContext("Iceberg", 1);
    var branch = BranchName.of("main");

    mockedAuthorizer.reset();
    soft.assertThat(catalog.dropNamespace(myNamespaceIceberg)).isFalse();
    soft.assertThat(mockedAuthorizer.checksWithoutIdentifiedKey())
        .containsExactly(
            authzCheck( // 'getEntries' in 'IcebergApiV1NamespaceResource.dropNamespace'
                apiContext, List.of(canReadEntries(branch)), Map.of()));

    mockedAuthorizer.reset();
    catalog.createNamespace(myNamespaceIceberg);
    soft.assertThat(mockedAuthorizer.checksWithoutIdentifiedKey())
        .containsExactly(
            authzCheck(apiContext, List.of(canViewReference(branch)), Map.of()),
            authzCheck( // 'commit'
                apiContext,
                List.of(
                    canViewReference(branch),
                    check(CREATE_ENTITY, branch, myNamespace, Set.of("CATALOG_CREATE_ENTITY")),
                    canCommitChangeAgainstReference(branch)),
                Map.of()));

    Map<String, String> meta = catalog.loadNamespaceMetadata(myNamespaceIceberg);
    String location = meta.get("location");

    var props = new HashMap<String, String>();
    props.put("location", location + "/foo_bar/baz");
    mockedAuthorizer.reset();
    catalog.createNamespace(myNamespaceIcebergInner, props);
    soft.assertThat(mockedAuthorizer.checksWithoutIdentifiedKey())
        .containsExactly(
            authzCheck(apiContext, List.of(canViewReference(branch)), Map.of()),
            authzCheck( // 'commit'
                apiContext,
                List.of(
                    canViewReference(branch),
                    check(
                        CREATE_ENTITY,
                        branch,
                        myNamespaceInner,
                        Set.of(
                            "META_SET_LOCATION", "CATALOG_CREATE_ENTITY", "META_SET_PROPERTIES")),
                    canCommitChangeAgainstReference(branch)),
                Map.of()));

    var props2 = new HashMap<String, String>();
    props2.put("a", "b");
    mockedAuthorizer.reset();
    catalog.setProperties(myNamespaceIceberg, props2);
    soft.assertThat(mockedAuthorizer.checksWithoutIdentifiedKey())
        .containsExactly(
            authzCheck( // 'getMultipleContents' in 'IcebergApiV1NamespaceResource.updateProperties'
                apiContext,
                List.of(
                    canViewReference(branch),
                    check(UPDATE_ENTITY, branch, myNamespace),
                    check(READ_ENTITY_VALUE, branch, myNamespace),
                    canCommitChangeAgainstReference(branch)),
                Map.of()),
            authzCheck( // 'commit'
                apiContext,
                List.of(
                    canViewReference(branch),
                    canCommitChangeAgainstReference(branch),
                    check(
                        UPDATE_ENTITY,
                        branch,
                        myNamespace,
                        Set.of("META_SET_PROPERTIES", "CATALOG_UPDATE_ENTITY"))),
                Map.of()));

    // not empty
    mockedAuthorizer.reset();
    soft.assertThatThrownBy(() -> catalog.dropNamespace(myNamespaceIceberg))
        .isInstanceOf(NamespaceNotEmptyException.class);
    soft.assertThat(mockedAuthorizer.checksWithoutIdentifiedKey())
        .containsExactly(
            authzCheck( // 'getEntries' in 'IcebergApiV1NamespaceResource.dropNamespace'
                apiContext,
                List.of(
                    canReadEntries(branch),
                    canReadContentKey(branch, myNamespace),
                    canReadContentKey(branch, myNamespaceInner)),
                Map.of()));

    mockedAuthorizer.reset();
    catalog.dropNamespace(myNamespaceIcebergInner);
    soft.assertThat(mockedAuthorizer.checksWithoutIdentifiedKey())
        .containsExactly(
            authzCheck( // 'getEntries' in 'IcebergApiV1NamespaceResource.dropNamespace'
                apiContext,
                List.of(canReadEntries(branch), canReadContentKey(branch, myNamespaceInner)),
                Map.of()),
            authzCheck( // 'commit'
                apiContext,
                List.of(
                    canViewReference(branch),
                    canCommitChangeAgainstReference(branch),
                    check(DELETE_ENTITY, branch, myNamespaceInner, Set.of("CATALOG_DROP_ENTITY"))),
                Map.of()));

    mockedAuthorizer.reset();
    catalog.dropNamespace(myNamespaceIceberg);
    soft.assertThat(mockedAuthorizer.checksWithoutIdentifiedKey())
        .containsExactly(
            authzCheck( // 'getEntries' in 'IcebergApiV1NamespaceResource.dropNamespace'
                apiContext,
                List.of(canReadEntries(branch), canReadContentKey(branch, myNamespace)),
                Map.of()),
            authzCheck( // 'commit'
                apiContext,
                List.of(
                    canViewReference(branch),
                    canCommitChangeAgainstReference(branch),
                    check(DELETE_ENTITY, branch, myNamespace, Set.of("CATALOG_DROP_ENTITY"))),
                Map.of()));
  }

  /** Verifies that the expected authz checks are issued with the right {@link ApiContext}. */
  @Test
  public void nessieApi() throws Exception {
    withClientCustomizer(
        c -> c.withAuthentication(BasicAuthenticationProvider.create("admin_user", "test123")));
    soft.assertThat(api().getAllReferences().stream()).isNotEmpty();

    // Verify that the ApiContext is correctly set
    soft.assertThat(mockedAuthorizer.checks())
        .containsExactly(
            authzCheck(
                apiContext("Nessie", 2),
                List.of(canViewReference(BranchName.of("main"))),
                Map.of()));
  }

  /**
   * Simulates how an authorizer implementation can disallow creating/updating Iceberg entities via
   * the Nessie API, but allow those via Iceberg, leveraging the {@code ApiContext}.
   */
  @Test
  public void commitFailWithNessieSucceedWithIceberg() {
    withClientCustomizer(
        c -> c.withAuthentication(BasicAuthenticationProvider.create("admin_user", "test123")));

    var catalog =
        catalog(Map.of("header.Authorization", basicAuthorizationHeader("admin_user", "test123")));

    var branch = BranchName.of("main");

    var myNamespaceKey = ContentKey.of("commit_nessie_iceberg");
    var myNamespaceIceberg = Namespace.of("commit_nessie_iceberg");
    var tableKey = ContentKey.of("commit_nessie_iceberg", "table_foo");
    var tableIdentifier = TableIdentifier.of(myNamespaceIceberg, "table_foo");

    var schema =
        new Schema(
            required(3, "id", Types.IntegerType.get()),
            required(4, "data", Types.StringType.get()));
    var spec = PartitionSpec.builderFor(schema).bucket("id", 16).build();

    BiFunction<BatchAccessChecker, Collection<Check>, Map<Check, String>> responder =
        (checker, checks) ->
            checks.stream()
                .filter(
                    c ->
                        (c.type() == CREATE_ENTITY || c.type() == UPDATE_ENTITY)
                            && checker.getApiContext().getApiName().equals("Nessie"))
                .collect(Collectors.toMap(Function.identity(), c -> "No no no"));

    // Creating a namespace is forbidden for Nessie API
    mockedAuthorizer.reset();
    mockedAuthorizer.setResponder(responder);
    soft.assertThatThrownBy(
            () ->
                api()
                    .createNamespace()
                    .refName(branch.getName())
                    .namespace(org.projectnessie.model.Namespace.of(myNamespaceKey))
                    .createWithResponse())
        .isInstanceOf(NessieForbiddenException.class)
        .hasMessageContaining("No no no");

    // Creating a namespace is allowed for Iceberg API
    mockedAuthorizer.reset();
    mockedAuthorizer.setResponder(responder);
    catalog.createNamespace(myNamespaceIceberg);

    // Updating a namespace is forbidden for Nessie API
    var props = new HashMap<String, String>();
    props.put("foo", "bar");
    mockedAuthorizer.reset();
    mockedAuthorizer.setResponder(responder);
    soft.assertThatThrownBy(
            () ->
                api()
                    .updateProperties()
                    .refName(branch.getName())
                    .namespace(org.projectnessie.model.Namespace.of(myNamespaceKey))
                    .updateProperties(props)
                    .updateWithResponse())
        .isInstanceOf(NessieForbiddenException.class)
        .hasMessageContaining("No no no");

    // Updating a namespace is allowed for Iceberg API
    mockedAuthorizer.reset();
    mockedAuthorizer.setResponder(responder);
    catalog.setProperties(myNamespaceIceberg, props);

    // Creating a table is forbidden for Nessie API
    mockedAuthorizer.reset();
    mockedAuthorizer.setResponder(responder);
    soft.assertThatThrownBy(
            () ->
                api()
                    .commitMultipleOperations()
                    .branch(api().getDefaultBranch())
                    .commitMeta(CommitMeta.fromMessage("attempt"))
                    .operation(
                        Operation.Put.of(tableKey, IcebergTable.of("ms://location", 1, 2, 3, 4)))
                    .commitWithResponse())
        .isInstanceOf(NessieForbiddenException.class)
        .hasMessageContaining("No no no");

    // Creating a table is allowed for Iceberg API
    mockedAuthorizer.reset();
    mockedAuthorizer.setResponder(responder);
    catalog.createTable(tableIdentifier, schema, spec, "my://location", props);
  }

  public static class Profile extends S3UnitTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return ImmutableMap.<String, String>builder()
          .putAll(super.getConfigOverrides())
          .putAll(BaseConfigProfile.CONFIG_OVERRIDES)
          .putAll(AUTH_CONFIG_OVERRIDES)
          .putAll(SECURITY_CONFIG)
          .put("quarkus.http.auth.basic", "true")
          // Need a dummy URL to satisfy the Quarkus OIDC extension.
          .put("quarkus.oidc.auth-server-url", "http://127.255.0.0:0/auth/realms/unset/")
          //
          .put("nessie.catalog.default-warehouse", WAREHOUSE_NAME)
          .put(CatalogProperties.WAREHOUSE_LOCATION, WAREHOUSE_NAME)
          //
          .put("nessie.server.authorization.enabled", "true")
          .put("nessie.server.authorization.type", "MOCKED")
          .build();
    }
  }

  public static String basicAuthorizationHeader(String username, String password) {
    String userPass = username + ':' + password;
    byte[] encoded = Base64.getEncoder().encode(userPass.getBytes(StandardCharsets.UTF_8));
    String encodedString = new String(encoded, StandardCharsets.UTF_8);
    return "Basic " + encodedString;
  }
}
