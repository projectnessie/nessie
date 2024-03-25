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

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.api.model.Pod;
import io.quarkus.test.common.QuarkusTestResource;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
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
import org.projectnessie.operator.reconciler.nessie.resource.Nessie;
import org.projectnessie.operator.testinfra.K3sContainerLifecycleManager;
import org.projectnessie.operator.testinfra.K3sContainerLifecycleManager.NessieIngressUri;
import org.projectnessie.operator.testinfra.K3sContainerLifecycleManager.NessieNodePortUri;
import org.projectnessie.operator.testinfra.K3sContainerLifecycleManager.PrometheusUri;

@QuarkusTestResource(value = K3sContainerLifecycleManager.class, parallel = true)
public abstract class AbstractNessieReconcilerIntegrationTests
    extends AbstractReconcilerIntegrationTests<Nessie> {

  private static final String NESSIE_INGRESS_HOST = "nessie.example.com";

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @NessieIngressUri protected URI nessieIngressUri;

  @NessieNodePortUri protected URI nessieNodePortUri;

  @PrometheusUri protected URI prometheusUri;

  protected NessieApiV2 nessieClient;

  @Override
  protected void setUpFunctionalTest() {
    nessieClient = nessieNodePortClient();
  }

  protected NessieApiV2 nessieNodePortClient() {
    return ((NessieHttpClientBuilder) NessieClientBuilder.createClientBuilderFromSystemSettings())
        .withUri(nessieNodePortUri)
        .withApiCompatibilityCheck(false)
        .build(NessieApiV2.class);
  }

  protected NessieApiV2 nessieIngressClient(HttpAuthentication authentication) {
    return ((NessieHttpClientBuilder) NessieClientBuilder.createClientBuilderFromSystemSettings())
        .withUri(nessieIngressUri)
        .withAuthentication(authentication)
        .addRequestFilter(ctx -> ctx.putHeader("Host", NESSIE_INGRESS_HOST))
        .withApiCompatibilityCheck(false)
        .build(NessieApiV2.class);
  }

  @Override
  protected void functionalTest() throws Exception {
    checkNessieOperational();
    if (primary.getSpec().serviceMonitor().enabled()) {
      checkServiceStatus();
    }
    if (primary.getSpec().telemetry().enabled()) {
      checkTelemetry();
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
                        + URLEncoder.encode("up{service=\"nessie-test\"}", StandardCharsets.UTF_8))
                .toURL(),
            JsonNode.class);
    assertThat(metrics.get("status").asText()).isEqualTo("success");
    JsonNode result = metrics.get("data").get("result");
    // A typical result looks like:
    // {
    //  "metric":{"__name__":"up","container":"nessie","endpoint":"nessie-server", ... },
    //  "value":[1.70956248969E9,"1"]
    // }
    assertThat(result)
        .anySatisfy(
            r -> assertThat(r.get("value").get(1).asText()).isEqualTo("1")); // "1" means "up"
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

  @AfterEach
  protected void closeNessieClient() {
    if (nessieClient != null) {
      nessieClient.close();
      nessieClient = null;
    }
  }
}
