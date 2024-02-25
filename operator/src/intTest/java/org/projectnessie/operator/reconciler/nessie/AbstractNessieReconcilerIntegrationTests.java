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
package org.projectnessie.operator.reconciler.nessie;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import org.junit.jupiter.api.AfterEach;
import org.projectnessie.client.NessieClientBuilder;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.client.http.HttpAuthentication;
import org.projectnessie.client.http.NessieHttpClientBuilder;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.CommitResponse;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.NessieConfiguration;
import org.projectnessie.model.Operation.Put;
import org.projectnessie.operator.reconciler.AbstractReconcilerIntegrationTests;
import org.projectnessie.operator.reconciler.nessie.dependent.ConfigMapDependent;
import org.projectnessie.operator.reconciler.nessie.dependent.DeploymentDependent;
import org.projectnessie.operator.reconciler.nessie.resource.Nessie;
import org.projectnessie.operator.testinfra.K3sContainerLifecycleManager.NessieUri;
import org.projectnessie.operator.testinfra.K3sContainerLifecycleManager.PrometheusUri;

public abstract class AbstractNessieReconcilerIntegrationTests
    extends AbstractReconcilerIntegrationTests<Nessie> {

  private static final String NESSIE_INGRESS_HOST = "nessie.example.com";

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @NessieUri protected URI nessieUri;

  @PrometheusUri protected URI prometheusUri;

  protected NessieApiV2 nessieClient;

  protected Deployment overrideConfigChecksum(Deployment deployment) {
    return deployment
        .edit()
        .editSpec()
        .editTemplate()
        .editMetadata()
        .addToAnnotations(
            DeploymentDependent.CONFIG_CHECKSUM_ANNOTATION,
            ConfigMapDependent.configChecksum(primary))
        .endMetadata()
        .endTemplate()
        .endSpec()
        .build();
  }

  @Override
  protected void setUpFunctionalTest() {
    nessieClient = nessieNodePortClient(null).build(NessieApiV2.class);
  }

  protected NessieHttpClientBuilder nessieNodePortClient(HttpAuthentication authentication) {
    return ((NessieHttpClientBuilder) NessieClientBuilder.createClientBuilderFromSystemSettings())
        .withUri(nessieUri)
        .withAuthentication(authentication)
        .withApiCompatibilityCheck(false);
  }

  protected NessieHttpClientBuilder nessieIngressClient() {
    return nessieNodePortClient(null)
        .addRequestFilter(ctx -> ctx.putHeader("Host", NESSIE_INGRESS_HOST));
  }

  @Override
  protected void functionalTest() throws Exception {
    checkNessieOperational();
    if (primary.getSpec().telemetry().enabled()) {
      checkTelemetry();
    }
    if (primary.getSpec().monitoring().enabled()) {
      checkServiceStatus();
    }
  }

  protected void checkNessieOperational() throws NessieNotFoundException, NessieConflictException {
    NessieConfiguration config = nessieClient.getConfig();
    Branch branch = (Branch) nessieClient.getReference().refName(config.getDefaultBranch()).get();
    String tableName = "table-" + System.nanoTime();
    ContentKey key = ContentKey.of(tableName);
    IcebergTable table = IcebergTable.of("irrelevant", 1, 2, 3, 4);
    CommitResponse response =
        nessieClient
            .commitMultipleOperations()
            .branch(branch)
            .commitMeta(CommitMeta.fromMessage("Add " + tableName))
            .operation(Put.of(key, table))
            .commitWithResponse();
    assertThat(response.getAddedContents()).isNotNull();
    assertThat(response.getAddedContents().size()).isOne();
  }

  protected void checkServiceStatus() throws IOException {
    JsonNode metrics =
        OBJECT_MAPPER.readValue(
            prometheusUri
                .resolve(
                    "/api/v1/query?query="
                        + URLEncoder.encode(
                            "nessie_versionstore_request_total{application=\"Nessie\",method=\"commit\"}",
                            UTF_8))
                .toURL(),
            JsonNode.class);
    assertThat(metrics.get("status").asText()).isEqualTo("success");
    JsonNode result = metrics.get("data").get("result");
    // A typical result looks like:
    // {
    //  "metric":{"__name__":"...","container":"nessie","endpoint":"nessie-test-mgmt", ... },
    //  "value":[1.70956248969E9,"1"]
    // }
    assertThat(result)
        .isNotEmpty()
        .anySatisfy(r -> assertThat(r.get("value").get(1).asInt()).isGreaterThanOrEqualTo(1));
  }

  protected void checkTelemetry() {
    // The otel-collector pod should have received traces, and it is configured with the
    // debug exporter, so we can check its logs for traces.
    Pod pod = client.pods().inNamespace("otel-collector").list().getItems().get(0);
    String logs = kubectl.logs(pod.getMetadata().getName(), "otel-collector");
    assertThat(logs)
        .contains("ObservingPersist.fetchReference")
        .contains("service.name: Str(nessie-test-custom)");
  }

  @Override
  protected void assertResourcesDeleted() {
    assertThat(get(client.serviceAccounts(), "nessie-test")).isNull();
    assertThat(get(client.apps().deployments(), "nessie-test")).isNull();
    assertThat(get(client.services(), "nessie-test")).isNull();
    assertThat(get(client.services(), "nessie-test-mgmt")).isNull();
    assertThat(get(client.network().v1().ingresses(), "nessie-test")).isNull();
    assertThat(get(client.monitoring().serviceMonitors(), "nessie-test")).isNull();
    assertThat(getPrimaryEventList().getItems()).isEmpty();
    assertThat(client.resource(primary).get()).isNull();
  }

  @AfterEach
  protected void closeNessieClient() {
    if (nessieClient != null) {
      nessieClient.close();
      nessieClient = null;
    }
  }
}
