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
package org.projectnessie.operator.reconciler.nessie.dependent;

import static org.projectnessie.operator.events.EventReason.DuplicateEnvVar;
import static org.projectnessie.operator.reconciler.nessie.dependent.ServiceAccountDependent.serviceAccountName;

import io.fabric8.kubernetes.api.model.ConfigMapVolumeSourceBuilder;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.ContainerPortBuilder;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.EnvVarSourceBuilder;
import io.fabric8.kubernetes.api.model.HTTPGetActionBuilder;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.KeyToPathBuilder;
import io.fabric8.kubernetes.api.model.LabelSelectorBuilder;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaimVolumeSource;
import io.fabric8.kubernetes.api.model.PodSpec;
import io.fabric8.kubernetes.api.model.PodSpecBuilder;
import io.fabric8.kubernetes.api.model.PodTemplateSpec;
import io.fabric8.kubernetes.api.model.PodTemplateSpecBuilder;
import io.fabric8.kubernetes.api.model.ProbeBuilder;
import io.fabric8.kubernetes.api.model.SecretKeySelectorBuilder;
import io.fabric8.kubernetes.api.model.SecretVolumeSourceBuilder;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.fabric8.kubernetes.api.model.apps.DeploymentSpec;
import io.fabric8.kubernetes.api.model.apps.DeploymentSpecBuilder;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.dependent.DependentResource;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.CRUDKubernetesDependentResource;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.KubernetesDependent;
import io.javaoperatorsdk.operator.processing.dependent.workflow.Condition;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import org.projectnessie.operator.events.EventReason;
import org.projectnessie.operator.events.EventService;
import org.projectnessie.operator.reconciler.KubernetesHelper;
import org.projectnessie.operator.reconciler.nessie.NessieReconciler;
import org.projectnessie.operator.reconciler.nessie.resource.Nessie;
import org.projectnessie.operator.reconciler.nessie.resource.options.AwsCredentials;
import org.projectnessie.operator.reconciler.nessie.resource.options.BigTableOptions;
import org.projectnessie.operator.reconciler.nessie.resource.options.CassandraOptions;
import org.projectnessie.operator.reconciler.nessie.resource.options.DynamoDbOptions;
import org.projectnessie.operator.reconciler.nessie.resource.options.ImageOptions;
import org.projectnessie.operator.reconciler.nessie.resource.options.JdbcOptions;
import org.projectnessie.operator.reconciler.nessie.resource.options.MongoDbOptions;
import org.projectnessie.operator.reconciler.nessie.resource.options.SecretValue;
import org.projectnessie.operator.reconciler.nessie.resource.options.ServiceOptions;
import org.projectnessie.operator.reconciler.nessie.resource.options.VersionStoreOptions.VersionStoreType;
import org.projectnessie.operator.reconciler.nessie.resource.options.WorkloadOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@KubernetesDependent(labelSelector = NessieReconciler.DEPENDENT_RESOURCES_SELECTOR)
public class DeploymentDependent extends CRUDKubernetesDependentResource<Deployment, Nessie> {

  public static final String CONFIG_CHECKSUM_ANNOTATION = "projectnessie.org/config-checksum";
  public static final String ROCKS_MOUNT_PATH = "/rocks-nessie";

  private static final Logger LOGGER = LoggerFactory.getLogger(DeploymentDependent.class);

  public DeploymentDependent() {
    super(Deployment.class);
  }

  @Override
  public Deployment create(Deployment desired, Nessie nessie, Context<Nessie> context) {
    LOGGER.debug(
        "Creating deployment {} for {}",
        desired.getMetadata().getName(),
        nessie.getMetadata().getName());
    EventService eventService = EventService.retrieveFromContext(context);
    eventService.fireEvent(
        nessie,
        EventReason.CreatingDeployment,
        "Creating deployment %s",
        desired.getMetadata().getName());
    return super.create(desired, nessie, context);
  }

  public Deployment desired(Nessie nessie, Context<Nessie> context) {
    KubernetesHelper helper = KubernetesHelper.retrieveFromContext(context);
    Deployment deployment =
        new DeploymentBuilder()
            .withMetadata(
                helper
                    .metaBuilder(nessie)
                    // also apply pod labels to the deployment (but not pod annotations)
                    .addToLabels(nessie.getSpec().deployment().labels())
                    .build())
            .withSpec(newDeploymentSpec(nessie, helper))
            .build();
    configureConfigMapMount(nessie, deployment);
    configureAuthentication(nessie, deployment);
    configureVersionStore(nessie, deployment);
    configureEnvVars(nessie, deployment, context);
    return deployment;
  }

  private DeploymentSpec newDeploymentSpec(Nessie nessie, KubernetesHelper helper) {
    Map<String, String> selectorLabels = helper.selectorLabels(nessie);
    return new DeploymentSpecBuilder()
        .withSelector(new LabelSelectorBuilder().withMatchLabels(selectorLabels).build())
        .withReplicas(nessie.getSpec().autoscaling().enabled() ? null : nessie.getSpec().size())
        .withTemplate(newPodTemplateSpec(nessie, selectorLabels))
        .build();
  }

  private PodTemplateSpec newPodTemplateSpec(Nessie nessie, Map<String, String> selectorLabels) {
    WorkloadOptions pod = nessie.getSpec().deployment();
    return new PodTemplateSpecBuilder()
        .withMetadata(
            new ObjectMetaBuilder()
                .withLabels(selectorLabels)
                .addToLabels(pod.labels())
                .withAnnotations(pod.annotations())
                .addToAnnotations(
                    CONFIG_CHECKSUM_ANNOTATION, ConfigMapDependent.configChecksum(nessie))
                .build())
        .withSpec(newPodSpec(nessie))
        .build();
  }

  private PodSpec newPodSpec(Nessie nessie) {
    WorkloadOptions pod = nessie.getSpec().deployment();
    return new PodSpecBuilder()
        .withServiceAccountName(serviceAccountName(nessie, pod.serviceAccount()))
        .withSecurityContext(pod.podSecurityContext())
        .withImagePullSecrets(
            pod.image().pullSecretRef() != null ? List.of(pod.image().pullSecretRef()) : List.of())
        .withNodeSelector(pod.nodeSelector())
        .withTolerations(pod.tolerations())
        .withAffinity(pod.affinity())
        .withContainers(newContainer(nessie))
        .build();
  }

  private Container newContainer(Nessie nessie) {
    WorkloadOptions pod = nessie.getSpec().deployment();
    ContainerBuilder containerBuilder =
        new ContainerBuilder()
            .withName("nessie")
            .withImage(pod.image().fullName(ImageOptions.DEFAULT_NESSIE_REPOSITORY))
            .withImagePullPolicy(Objects.requireNonNull(pod.image().pullPolicy()).name())
            .withResources(pod.resources())
            .withSecurityContext(pod.containerSecurityContext())
            .withPorts(
                new ContainerPortBuilder()
                    .withName(MainServiceDependent.PORT_NAME)
                    .withContainerPort(ServiceOptions.DEFAULT_NESSIE_PORT)
                    .withProtocol("TCP")
                    .build(),
                new ContainerPortBuilder()
                    .withName(ManagementServiceDependent.PORT_NAME)
                    .withContainerPort(ManagementServiceDependent.PORT_NUMBER)
                    .withProtocol("TCP")
                    .build())
            .withLivenessProbe(
                new ProbeBuilder()
                    .withHttpGet(
                        new HTTPGetActionBuilder()
                            .withPath("/q/health/live")
                            .withPort(new IntOrString(ManagementServiceDependent.PORT_NAME))
                            .withScheme("HTTP")
                            .build())
                    .withInitialDelaySeconds(pod.livenessProbe().initialDelaySeconds())
                    .withPeriodSeconds(pod.livenessProbe().periodSeconds())
                    .withTimeoutSeconds(pod.livenessProbe().timeoutSeconds())
                    .withFailureThreshold(pod.livenessProbe().failureThreshold())
                    .withSuccessThreshold(pod.livenessProbe().successThreshold())
                    .build())
            .withReadinessProbe(
                new ProbeBuilder()
                    .withHttpGet(
                        new HTTPGetActionBuilder()
                            .withPath("/q/health/ready")
                            .withPort(new IntOrString(ManagementServiceDependent.PORT_NAME))
                            .withScheme("HTTP")
                            .build())
                    .withInitialDelaySeconds(pod.readinessProbe().initialDelaySeconds())
                    .withPeriodSeconds(pod.readinessProbe().periodSeconds())
                    .withTimeoutSeconds(pod.readinessProbe().timeoutSeconds())
                    .withFailureThreshold(pod.readinessProbe().failureThreshold())
                    .withSuccessThreshold(pod.readinessProbe().successThreshold())
                    .build());

    if (nessie.getSpec().remoteDebug().enabled()) {
      containerBuilder.addToPorts(
          new ContainerPortBuilder()
              .withContainerPort(nessie.getSpec().remoteDebug().port())
              .withName("nessie-debug")
              .withProtocol("TCP")
              .build());
    }

    return containerBuilder.build();
  }

  private static void configureConfigMapMount(Nessie nessie, Deployment deployment) {
    PodSpec pod = deployment.getSpec().getTemplate().getSpec();
    Container container = pod.getContainers().get(0);
    container
        .getVolumeMounts()
        .add(
            new VolumeMountBuilder()
                .withName("nessie-config")
                .withMountPath("/deployments/config/application.properties")
                .withSubPath("application.properties")
                .build());
    pod.getVolumes()
        .add(
            new VolumeBuilder()
                .withName("nessie-config")
                .withConfigMap(
                    new ConfigMapVolumeSourceBuilder()
                        .withName(nessie.getMetadata().getName())
                        .withOptional(false)
                        .build())
                .build());
  }

  private static void configureAuthentication(Nessie nessie, Deployment deployment) {
    if (nessie.getSpec().authentication().enabled()) {
      SecretValue secretValue = nessie.getSpec().authentication().oidcClientSecret();
      if (secretValue != null) {
        PodSpec pod = deployment.getSpec().getTemplate().getSpec();
        Container container = pod.getContainers().get(0);
        container.getEnv().add(envVarFromSecret("quarkus.oidc.credentials.secret", secretValue));
      }
    }
  }

  private static void configureVersionStore(Nessie nessie, Deployment deployment) {
    PodSpec pod = deployment.getSpec().getTemplate().getSpec();
    Container container = pod.getContainers().get(0);
    VersionStoreType type = nessie.getSpec().versionStore().type();
    switch (type) {
      case InMemory -> {}
      case RocksDb -> configureRocks(nessie, container, pod.getVolumes());
      case Jdbc -> configureJdbc(nessie, container);
      case BigTable -> configureBigTable(nessie, container, pod.getVolumes());
      case MongoDb -> configureMongo(nessie, container);
      case Cassandra -> configureCassandra(nessie, container);
      case DynamoDb -> configureDynamo(nessie, container);
      default -> throw new AssertionError("Unexpected version store type: " + type);
    }
  }

  private static void configureRocks(Nessie nessie, Container container, List<Volume> volumes) {
    container
        .getVolumeMounts()
        .add(
            new VolumeMountBuilder()
                .withName("rocks-storage")
                .withMountPath(ROCKS_MOUNT_PATH)
                .build());
    // Note: readOnly: false creates an infinite reconcile loop, because the actual deployment
    // will contain readOnly: null regardless of the value in the desired deployment.
    PersistentVolumeClaimVolumeSource claim =
        new PersistentVolumeClaimVolumeSource(nessie.getMetadata().getName(), null);
    volumes.add(
        new VolumeBuilder().withName("rocks-storage").withPersistentVolumeClaim(claim).build());
  }

  private static void configureJdbc(Nessie nessie, Container container) {
    JdbcOptions jdbc = nessie.getSpec().versionStore().jdbc();
    if (jdbc != null && jdbc.password() != null) {
      EnvVar password =
          envVarFromSecret(jdbc.datasource().configPrefix() + "password", jdbc.password());
      container.getEnv().add(password);
    }
  }

  private static void configureBigTable(Nessie nessie, Container container, List<Volume> volumes) {
    BigTableOptions bigTable = nessie.getSpec().versionStore().bigTable();
    if (bigTable != null && bigTable.credentials() != null) {
      container
          .getEnv()
          .add(
              new EnvVar(
                  "GOOGLE_APPLICATION_CREDENTIALS", "/bigtable-nessie/sa_credentials.json", null));
      container
          .getVolumeMounts()
          .add(
              new VolumeMountBuilder()
                  .withName("bigtable-creds")
                  .withMountPath("/bigtable-nessie")
                  .build());
      volumes.add(
          new VolumeBuilder()
              .withName("bigtable-creds")
              .withSecret(
                  new SecretVolumeSourceBuilder()
                      .withSecretName(bigTable.credentials().secret())
                      .withItems(
                          new KeyToPathBuilder()
                              .withKey(bigTable.credentials().key())
                              .withPath("sa_credentials.json")
                              .build())
                      .build())
              .build());
    }
  }

  private static void configureMongo(Nessie nessie, Container container) {
    MongoDbOptions mongoDb = nessie.getSpec().versionStore().mongoDb();
    if (mongoDb != null && mongoDb.password() != null) {
      container
          .getEnv()
          .add(envVarFromSecret("quarkus.mongodb.credentials.password", mongoDb.password()));
    }
  }

  private static void configureCassandra(Nessie nessie, Container container) {
    CassandraOptions cassandra = nessie.getSpec().versionStore().cassandra();
    if (cassandra != null && cassandra.password() != null) {
      container
          .getEnv()
          .add(envVarFromSecret("quarkus.cassandra.auth.password", cassandra.password()));
    }
  }

  private static void configureDynamo(Nessie nessie, Container container) {
    DynamoDbOptions dynamoDb = nessie.getSpec().versionStore().dynamoDb();
    if (dynamoDb != null) {
      AwsCredentials credentials = dynamoDb.credentials();
      container
          .getEnv()
          .add(
              envVarFromSecret(
                  "AWS_ACCESS_KEY_ID", credentials.secret(), credentials.accessKeyId()));
      container
          .getEnv()
          .add(
              envVarFromSecret(
                  "AWS_SECRET_ACCESS_KEY", credentials.secret(), credentials.secretAccessKey()));
    }
  }

  private static void configureEnvVars(
      Nessie nessie, Deployment deployment, Context<Nessie> context) {
    List<EnvVar> env = new ArrayList<>();
    addJvmOptionsEnvVar(nessie, env);
    addDebugEnvVars(nessie, env);
    addExtraEnvVars(nessie, env);
    Map<String, EnvVar> map = new TreeMap<>();
    for (EnvVar envVar : env) {
      EnvVar old = map.put(envVar.getName(), envVar);
      if (old != null) {
        EventService.retrieveFromContext(context)
            .fireEvent(
                nessie, DuplicateEnvVar, "Duplicate environment variable: %s", envVar.getName());
      }
    }
    deployment
        .getSpec()
        .getTemplate()
        .getSpec()
        .getContainers()
        .get(0)
        .getEnv()
        .addAll(map.values());
  }

  private static void addJvmOptionsEnvVar(Nessie nessie, List<EnvVar> env) {
    nessie.getSpec().jvmOptions().stream()
        .map(Objects::toString)
        .reduce((a, b) -> a + " " + b)
        .ifPresent(s -> env.add(new EnvVar("JAVA_OPTS_APPEND", s, null)));
  }

  private static void addDebugEnvVars(Nessie nessie, List<EnvVar> env) {
    if (nessie.getSpec().remoteDebug().enabled()) {
      env.add(new EnvVar("JAVA_DEBUG", "true", null));
      // Use * to bind to all interfaces
      env.add(new EnvVar("JAVA_DEBUG_PORT", "*:" + nessie.getSpec().remoteDebug().port(), null));
    }
  }

  private static void addExtraEnvVars(Nessie nessie, List<EnvVar> env) {
    if (nessie.getSpec().extraEnv() != null) {
      env.addAll(nessie.getSpec().extraEnv());
    }
  }

  private static EnvVar envVarFromSecret(String name, SecretValue secretValue) {
    return envVarFromSecret(name, secretValue.secret(), secretValue.key());
  }

  private static EnvVar envVarFromSecret(String name, String secretRef, String key) {
    return new EnvVarBuilder()
        .withName(name)
        .withValueFrom(
            new EnvVarSourceBuilder()
                .withSecretKeyRef(
                    new SecretKeySelectorBuilder().withName(secretRef).withKey(key).build())
                .build())
        .build();
  }

  public static class ReadyCondition implements Condition<Deployment, Nessie> {

    @Override
    public boolean isMet(
        DependentResource<Deployment, Nessie> dependentResource,
        Nessie nessie,
        Context<Nessie> context) {
      return dependentResource
          .getSecondaryResource(nessie, context)
          .map(
              d ->
                  nessie.getSpec().autoscaling().enabled()
                      || (d.getStatus() != null
                          && Objects.equals(
                              d.getStatus().getAvailableReplicas(), nessie.getSpec().size())))
          .orElse(false);
    }
  }
}
