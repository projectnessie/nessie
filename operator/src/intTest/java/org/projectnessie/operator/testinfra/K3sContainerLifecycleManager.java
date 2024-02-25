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
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.dsl.NonDeletingOperation;
import io.fabric8.openshift.client.OpenShiftClient;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager.TestInjector.Annotated;
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
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Stream;
import org.intellij.lang.annotations.Language;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.k3s.K3sContainer;

public class K3sContainerLifecycleManager implements QuarkusTestResourceLifecycleManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(K3sContainerLifecycleManager.class);

  private static final int NESSIE_INGRESS_PORT = 80;
  private static final int PROMETHEUS_NODE_PORT = 30090;
  private static final int NESSIE_NODE_PORT = 30120;

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
            set:
              alertmanager.enabled: "false"
              blackboxExporter.enabled: "false"
              exporters.node-exporter.enabled: "false"
              exporters.kube-state-metrics.enabled: "false"
              operator.serviceMonitor.enabled: "false"
              prometheus.serviceMonitor.enabled: "false"
              prometheus.service.type: NodePort
              prometheus.service.nodePorts.http: %d
          """
          .formatted(PROMETHEUS_NODE_PORT);

  @Language("YAML")
  private static final String OTEL_COLLECTOR_HELM_CHART =
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

  @Target(ElementType.FIELD)
  @Retention(RetentionPolicy.RUNTIME)
  public @interface NessieIngressUri {}

  @Target(ElementType.FIELD)
  @Retention(RetentionPolicy.RUNTIME)
  public @interface NessieNodePortUri {}

  @Target(ElementType.FIELD)
  @Retention(RetentionPolicy.RUNTIME)
  public @interface PrometheusUri {}

  private static K3sContainer k3s;
  private static URI nessieIngressUri;
  private static URI nessieNodePortUri;
  private static URI prometheusUri;

  private static OpenShiftClient k8sClient;

  @Override
  public Map<String, String> start() {
    if (k3s == null) {
      k3s =
          new K3sContainer(ContainerImages.K3S.image())
              .withNetwork(Network.SHARED)
              .withLogConsumer(new Slf4jLogConsumer(LOGGER))
              .withStartupAttempts(3)
              .withCopyToContainer(
                  Transferable.of(PROMETHEUS_HELM_CHART),
                  "/var/lib/rancher/k3s/server/manifests/prometheus.yaml")
              .withCopyToContainer(
                  Transferable.of(OTEL_COLLECTOR_HELM_CHART),
                  "/var/lib/rancher/k3s/server/manifests/otel-collector.yaml");
      // override default command to enable Traefik
      k3s.setCommand("server", "--tls-san=" + k3s.getHost());
      k3s.addExposedPorts(NESSIE_INGRESS_PORT, PROMETHEUS_NODE_PORT, NESSIE_NODE_PORT);
      k3s.start();
      setUpK8sClient();
      installCrds();
      waitForTraefikReady();
      waitForPrometheusReady();
      waitForOpenTelemetryCollectorReady();
      nessieIngressUri =
          URI.create(
              "http://localhost:%d/api/v2".formatted(k3s.getMappedPort(NESSIE_INGRESS_PORT)));
      nessieNodePortUri =
          URI.create("http://localhost:%d/api/v2".formatted(k3s.getMappedPort(NESSIE_NODE_PORT)));
      prometheusUri =
          URI.create("http://localhost:%d".formatted(k3s.getMappedPort(PROMETHEUS_NODE_PORT)));
    }
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
  public void stop() {
    // leave the K3s container and the K8s client running for the next test
  }

  @Override
  public void inject(TestInjector testInjector) {
    testInjector.injectIntoFields(new Kubectl(), new MatchesType(Kubectl.class));
    testInjector.injectIntoFields(k8sClient, new MatchesType(OpenShiftClient.class));
    testInjector.injectIntoFields(
        nessieIngressUri, new MatchesType(URI.class).and(new Annotated(NessieIngressUri.class)));
    testInjector.injectIntoFields(
        nessieNodePortUri, new MatchesType(URI.class).and(new Annotated(NessieNodePortUri.class)));
    testInjector.injectIntoFields(
        prometheusUri, new MatchesType(URI.class).and(new Annotated(PrometheusUri.class)));
  }

  @SuppressWarnings("resource")
  private void setUpK8sClient() {
    String kubeConfigYaml = k3s.getKubeConfigYaml();
    Config config = Config.fromKubeconfig(kubeConfigYaml);
    k8sClient =
        new KubernetesClientBuilder().withConfig(config).build().adapt(OpenShiftClient.class);
  }

  private void installCrds() {
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

  private void waitForTraefikReady() {
    LOGGER.info("Waiting for Ingress to be ready");
    new Kubectl()
        .waitUntil(
            "pod",
            "kube-system",
            "Ready",
            Duration.ofMinutes(2),
            "--selector=app.kubernetes.io/instance=traefik-kube-system");
  }

  private void waitForPrometheusReady() {
    LOGGER.info("Waiting for Prometheus to be ready");
    new Kubectl()
        .waitUntil(
            "pod",
            "prometheus",
            "Ready",
            Duration.ofMinutes(2),
            "--selector=app.kubernetes.io/instance=prometheus");
  }

  private void waitForOpenTelemetryCollectorReady() {
    LOGGER.info("Waiting for OpenTelemetry collector to be ready");
    new Kubectl()
        .waitUntil(
            "pod",
            "otel-collector",
            "Ready",
            Duration.ofMinutes(2),
            "--selector=app.kubernetes.io/instance=otel-collector");
  }

  public static class Kubectl {

    public ExecResult exec(String... args) {
      String[] cmd = new String[args.length + 1];
      cmd[0] = "kubectl";
      System.arraycopy(args, 0, cmd, 1, args.length);
      ExecResult result;
      try {
        // Run kubectl command in the main container, no need to use a sidecar
        result = k3s.execInContainer(cmd);
      } catch (IOException | InterruptedException e) {
        throw new RuntimeException(e);
      }
      if (result.getExitCode() != 0) {
        throw new KubectlExecException(cmd, result);
      }
      return result;
    }

    public void deleteAll(String namespace) {
      exec("delete", "all", "--all", "--namespace", namespace, "--force");
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

    public String logs(String name, String namespace) {
      return exec("logs", name, "--namespace=" + namespace).getStdout();
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
