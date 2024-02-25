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
package org.projectnessie.operator.testinfra;

import static org.assertj.core.api.Fail.fail;
import static org.awaitility.Awaitility.await;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.dsl.NonDeletingOperation;
import io.fabric8.openshift.client.OpenShiftClient;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager.TestInjector.AnnotatedAndMatchesType;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager.TestInjector.MatchesType;
import java.io.IOException;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.intellij.lang.annotations.Language;
import org.projectnessie.api.NessieVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.k3s.K3sContainer;

public class K3sContainerLifecycleManager extends AbstractContainerLifecycleManager<K3sContainer> {

  private static final Logger LOGGER = LoggerFactory.getLogger(K3sContainerLifecycleManager.class);

  private static final int NESSIE_INGRESS_PORT = 80;
  private static final int PROMETHEUS_NODE_PORT = 30090;
  private static final int NESSIE_NODE_PORT = 30120;

  @Language("YAML")
  private static final String TRAEFIK_HELM_CHART =
      """
      apiVersion: helm.cattle.io/v1
      kind: HelmChartConfig
      metadata:
        name: traefik
        namespace: kube-system
      spec:
        valuesContent: |-
          ingressRoute:
            dashboard:
              enabled: false
            healthcheck:
              enabled: false
          providers:
            kubernetesCRD:
              enabled: false
          metrics:
            addInternals: false
            prometheus: null
          resources:
            requests:
              cpu: "300m"
              memory: "150Mi"
            limits:
              cpu: "300m"
              memory: "150Mi"
          livenessProbe:
            initialDelaySeconds: 2
            failureThreshold: 30
            periodSeconds: 1
          readinessProbe:
            initialDelaySeconds: 2
            failureThreshold: 30
            periodSeconds: 1
      """;

  @Language("YAML")
  private static final String PROMETHEUS_HELM_CHART =
      """
      apiVersion: helm.cattle.io/v1
      kind: HelmChart
      metadata:
        name: prometheus
        namespace: kube-system
      spec:
        repo: https://charts.bitnami.com/bitnami
        chart: kube-prometheus
        targetNamespace: prometheus
        createNamespace: true
        valuesContent: |-
          prometheus.serviceMonitorSelector:
            app.kubernetes.io/component: nessie
        set:
          alertmanager.enabled: "false"
          blackboxExporter.enabled: "false"
          coreDns.enabled: "false"
          coreDns.service.enabled: "false"
          exporters.node-exporter.enabled: "false"
          exporters.kube-state-metrics.enabled: "false"
          kubelet.enabled: "false"
          kubeApiServer.enabled: "false"
          kubeControllerManager.enabled: "false"
          kubeControllerManager.service.enabled: "false"
          kubeProxy.enabled: "false"
          kubeProxy.service.enabled: "false"
          kubeScheduler.enabled: "false"
          kubeScheduler.service.enabled: "false"
          operator.serviceMonitor.enabled: "false"
          operator.kubeletService.enabled: "false"
          prometheus.configReloader.service.enabled: "false"
          prometheus.scrapeInterval: "1s"
          prometheus.serviceMonitor.enabled: "false"
          prometheus.service.type: NodePort
          prometheus.service.nodePorts.http: %d
      """;

  @Language("YAML")
  private static final String COLLECTOR_HELM_CHART =
      """
      apiVersion: helm.cattle.io/v1
      kind: HelmChart
      metadata:
        name: otel-collector
        namespace: kube-system
      spec:
        repo: https://open-telemetry.github.io/opentelemetry-helm-charts
        chart: opentelemetry-collector
        targetNamespace: otel-collector
        createNamespace: true
        valuesContent: |-
          mode: deployment
          image:
            repository: "otel/opentelemetry-collector-k8s"
          ports:
           jaeger-compact:
             enabled: false
           jaeger-thrift:
             enabled: false
           jaeger-grpc:
             enabled: false
           zipkin:
             enabled: false
          config:
           receivers:
             jaeger: null
             prometheus: null
             zipkin: null
           exporters:
             debug:
               verbosity: detailed
               sampling_initial: 1
               sampling_thereafter: 1
           service:
             pipelines:
               traces:
                 exporters:
                   - debug
                 receivers:
                   - otlp
               logs: null
               metrics: null
      """;

  @Language("Shell Script")
  private static final String IMAGE_IMPORT_SCRIPT =
      """
      #!/usr/bin/env bash
      set -e
      TOOL="$(which docker > /dev/null && echo docker || echo podman)"
      ${TOOL} image save projectnessie/nessie-test-server:$NESSIE_VERSION | \
        ${TOOL} exec --interactive $CONTAINER_NAME ctr images import --no-unpack -
      """;

  @Target(ElementType.FIELD)
  @Retention(RetentionPolicy.RUNTIME)
  public @interface NessieUri {}

  @Target(ElementType.FIELD)
  @Retention(RetentionPolicy.RUNTIME)
  public @interface PrometheusUri {}

  private URI nessieUri;
  private URI prometheusUri;

  private OpenShiftClient k8sClient;

  private boolean monitoring;
  private boolean ingress;
  private boolean telemetry;
  private boolean waitForComponents;

  @Override
  public void init(Map<String, String> initArgs) {
    monitoring = Boolean.parseBoolean(initArgs.getOrDefault("monitoring", "false"));
    ingress = Boolean.parseBoolean(initArgs.getOrDefault("ingress", "false"));
    telemetry = Boolean.parseBoolean(initArgs.getOrDefault("telemetry", "false"));
    waitForComponents = Boolean.parseBoolean(initArgs.getOrDefault("waitForComponents", "false"));
  }

  @Override
  protected K3sContainer createContainer() {
    K3sContainer container =
        new K3sContainer(dockerImage("k3s").asCompatibleSubstituteFor("rancher/k3s"));
    List<Integer> exposedPorts = new ArrayList<>();
    List<String> commandParts = new ArrayList<>();
    commandParts.add("server");
    commandParts.add("--tls-san=" + container.getHost());
    // Mitigate eviction issues in CI by setting eviction thresholds for nodefs very low
    commandParts.add("--kubelet-arg=eviction-hard=nodefs.available<1%,nodefs.inodesFree<1%");
    // Enable rootless containers
    commandParts.add("--kubelet-arg=feature-gates=KubeletInUserNamespace=true");
    if (monitoring) {
      container.withCopyToContainer(
          Transferable.of(PROMETHEUS_HELM_CHART.formatted(PROMETHEUS_NODE_PORT)),
          "/var/lib/rancher/k3s/server/manifests/prometheus.yaml");
      exposedPorts.add(PROMETHEUS_NODE_PORT);
    }
    if (ingress) {
      container.withCopyToContainer(
          Transferable.of(TRAEFIK_HELM_CHART),
          "/var/lib/rancher/k3s/server/manifests/traefik-config.yaml");
      exposedPorts.add(NESSIE_INGRESS_PORT);
    } else {
      commandParts.add("--disable=traefik");
      exposedPorts.add(NESSIE_NODE_PORT);
    }
    if (telemetry) {
      container.withCopyToContainer(
          Transferable.of(COLLECTOR_HELM_CHART),
          "/var/lib/rancher/k3s/server/manifests/otel-collector.yaml");
    }
    container.setCommand(commandParts.toArray(new String[0]));
    container.addExposedPorts(exposedPorts.stream().mapToInt(Integer::intValue).toArray());
    return container;
  }

  @Override
  protected Map<String, String> quarkusConfig() {
    loadNessieImage();
    setUpK8sClient();
    installCrds();
    setUpUris();
    Config config = k8sClient.getConfiguration();
    return Map.of(
        "quarkus.kubernetes-client.api-server-url",
        config.getMasterUrl(),
        "quarkus.kubernetes-client.ca-cert-data",
        config.getCaCertData(),
        "quarkus.kubernetes-client.client-cert-data",
        config.getClientCertData(),
        "quarkus.kubernetes-client.client-key-data",
        config.getClientKeyData(),
        "quarkus.kubernetes-client.client-key-passphrase",
        config.getClientKeyPassphrase(),
        "quarkus.kubernetes-client.client-key-algo",
        config.getClientKeyAlgo(),
        "quarkus.kubernetes-client.namespace",
        "default");
  }

  @Override
  public void inject(TestInjector testInjector) {
    testInjector.injectIntoFields(new Kubectl(), new MatchesType(Kubectl.class));
    testInjector.injectIntoFields(k8sClient, new MatchesType(OpenShiftClient.class));
    testInjector.injectIntoFields(
        nessieUri, new AnnotatedAndMatchesType(NessieUri.class, URI.class));
    testInjector.injectIntoFields(
        prometheusUri, new AnnotatedAndMatchesType(PrometheusUri.class, URI.class));
  }

  @Override
  public void stop() {
    try {
      if (k8sClient != null) {
        k8sClient.close();
      }
    } finally {
      super.stop();
    }
  }

  private void loadNessieImage() {
    LOGGER.info("Importing Nessie server image into K3S node...");
    ProcessBuilder pb = new ProcessBuilder("bash", "-c", IMAGE_IMPORT_SCRIPT);
    pb.environment().put("NESSIE_VERSION", NessieVersion.NESSIE_VERSION);
    pb.environment().put("CONTAINER_NAME", container.getContainerName());
    try {
      Process process = pb.inheritIO().start();
      process.waitFor();
      if (process.exitValue() != 0) {
        throw new RuntimeException("Failed to import Nessie image");
      }
    } catch (IOException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings("resource")
  private void setUpK8sClient() {
    LOGGER.info("Setting up Kubernetes client...");
    String kubeConfigYaml = container.getKubeConfigYaml();
    Config config = Config.fromKubeconfig(kubeConfigYaml);
    k8sClient =
        new KubernetesClientBuilder().withConfig(config).build().adapt(OpenShiftClient.class);
  }

  private void installCrds() {
    LOGGER.info("Installing Nessie CRDs...");
    // quarkus.operator-sdk.crd.apply is not effective when running integration tests,
    // so we need to install the CRDs manually
    Path crdDir = Paths.get(System.getProperty("nessie.crds.dir", "build/kubernetes"));
    try (Stream<Path> walk = Files.walk(crdDir)) {
      walk.filter(Files::isRegularFile)
          .filter(path -> path.getFileName().toString().endsWith(".projectnessie.org-v1.yml"))
          .forEach(
              path ->
                  k8sClient
                      .apiextensions()
                      .v1()
                      .customResourceDefinitions()
                      .load(path.toFile())
                      .createOr(NonDeletingOperation::update));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void setUpUris() {
    if (monitoring) {
      if (waitForComponents) {
        waitForPrometheusReady();
      }
      prometheusUri =
          URI.create(
              "http://localhost:%d".formatted(container.getMappedPort(PROMETHEUS_NODE_PORT)));
    }
    if (telemetry) {
      if (waitForComponents) {
        waitForCollectorReady();
      }
    }
    if (ingress) {
      if (waitForComponents) {
        waitForTraefikReady();
      }
      nessieUri =
          URI.create(
              "http://localhost:%d/api/v2".formatted(container.getMappedPort(NESSIE_INGRESS_PORT)));
    } else {
      nessieUri =
          URI.create(
              "http://localhost:%d/api/v2".formatted(container.getMappedPort(NESSIE_NODE_PORT)));
    }
  }

  private void waitForPrometheusReady() {
    LOGGER.info("Waiting for Prometheus to be ready...");
    new Kubectl()
        .waitUntil(
            "pod",
            "prometheus",
            "Ready",
            Duration.ofMinutes(2),
            "--selector=app.kubernetes.io/instance=prometheus-kube-prometheus-prometheus");
  }

  private void waitForCollectorReady() {
    LOGGER.info("Waiting for OpenTelemetry collector to be ready...");
    new Kubectl()
        .waitUntil(
            "pod",
            "otel-collector",
            "Ready",
            Duration.ofMinutes(2),
            "--selector=app.kubernetes.io/instance=otel-collector");
  }

  private void waitForTraefikReady() {
    LOGGER.info("Waiting for Ingress to be ready...");
    new Kubectl()
        .waitUntil(
            "pod",
            "kube-system",
            "Ready",
            Duration.ofMinutes(2),
            "--selector=app.kubernetes.io/instance=traefik-kube-system");
  }

  public class Kubectl {

    public ExecResult exec(String... args) {
      String[] cmd = new String[args.length + 1];
      cmd[0] = "kubectl";
      System.arraycopy(args, 0, cmd, 1, args.length);
      ExecResult result;
      try {
        // Run kubectl command in the main container, no need to use a sidecar
        result = container.execInContainer(cmd);
      } catch (IOException | InterruptedException e) {
        throw new RuntimeException(e);
      }
      if (result.getExitCode() != 0) {
        throw new KubectlExecException(cmd, result);
      }
      return result;
    }

    public void deleteAll(String namespace, Duration timeout) {
      exec(
          "delete",
          "all",
          "--all",
          "--namespace",
          namespace,
          "--wait",
          "--timeout=%ds".formatted(timeout.getSeconds()));
    }

    public void waitUntil(
        HasMetadata resource,
        String namespace,
        String condition,
        Duration timeout,
        String... args) {
      waitUntil(
          resource.getKind() + "/" + resource.getMetadata().getName(),
          namespace,
          condition,
          timeout,
          args);
    }

    public void waitUntil(
        String name, String namespace, String condition, Duration timeout, String... args) {
      String[] cmd = new String[args.length + 5];
      cmd[0] = "wait";
      cmd[1] = "--for=condition=" + condition;
      cmd[2] = name;
      cmd[3] = "--timeout=%ds".formatted(timeout.getSeconds());
      cmd[4] = "--namespace=" + namespace;
      System.arraycopy(args, 0, cmd, 5, args.length);
      await()
          .atMost(timeout)
          .pollInterval(Duration.ofSeconds(1))
          .untilAsserted(
              () -> {
                try {
                  exec(cmd);
                } catch (KubectlExecException e) {
                  if (e.getResult().getStderr().contains("no matching resources found")) {
                    fail(e.getMessage()); // retry until at least one resource is found
                  }
                  throw e;
                }
              });
    }

    public String logs(Pod resource, boolean previous) {
      try {
        return logs(
            resource.getMetadata().getName(),
            resource.getMetadata().getNamespace(),
            "--previous=" + previous);
      } catch (KubectlExecException e) {
        if (e.getMessage().contains("not found")) {
          return "";
        }
        throw e;
      }
    }

    public String logs(String name, String namespace, String... args) {
      String[] cmd = new String[args.length + 3];
      cmd[0] = "logs";
      cmd[1] = name;
      cmd[2] = "--namespace=" + namespace;
      System.arraycopy(args, 0, cmd, 3, args.length);
      return exec(cmd).getStdout();
    }

    public String apiServerLogs() {
      return container.getLogs();
    }
  }

  public static class KubectlExecException extends RuntimeException {

    private final ExecResult result;

    public KubectlExecException(String[] cmd, ExecResult result) {
      super("command failed: %s: %s".formatted(Arrays.toString(cmd), result));
      this.result = result;
    }

    public ExecResult getResult() {
      return result;
    }
  }
}
