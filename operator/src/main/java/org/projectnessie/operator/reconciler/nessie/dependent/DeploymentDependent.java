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

import static org.projectnessie.operator.reconciler.nessie.dependent.ServiceAccountDependent.serviceAccountName;

import io.fabric8.kubernetes.api.model.ConfigMapEnvSource;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.ContainerPortBuilder;
import io.fabric8.kubernetes.api.model.EnvFromSourceBuilder;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.EnvVarSourceBuilder;
import io.fabric8.kubernetes.api.model.KeyToPathBuilder;
import io.fabric8.kubernetes.api.model.LabelSelectorBuilder;
import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaimVolumeSource;
import io.fabric8.kubernetes.api.model.PodSpec;
import io.fabric8.kubernetes.api.model.PodSpecBuilder;
import io.fabric8.kubernetes.api.model.PodTemplateSpec;
import io.fabric8.kubernetes.api.model.PodTemplateSpecBuilder;
import io.fabric8.kubernetes.api.model.SecretKeySelectorBuilder;
import io.fabric8.kubernetes.api.model.SecretVolumeSourceBuilder;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.api.model.VolumeMount;
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
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
import org.projectnessie.operator.reconciler.nessie.resource.options.ServiceOptions;
import org.projectnessie.operator.reconciler.nessie.resource.options.VersionStoreOptions.VersionStoreType;
import org.projectnessie.operator.reconciler.nessie.resource.options.WorkloadOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@KubernetesDependent(labelSelector = NessieReconciler.DEPENDENT_RESOURCES_SELECTOR)
public class DeploymentDependent extends CRUDKubernetesDependentResource<Deployment, Nessie> {

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
    configureVersionStore(nessie, deployment);
    configureExtraEnv(nessie, deployment);
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
            .withEnvFrom(
                new EnvFromSourceBuilder()
                    .withConfigMapRef(new ConfigMapEnvSource(nessie.getMetadata().getName(), false))
                    .build())
            .withPorts(
                new ContainerPortBuilder()
                    .withContainerPort(ServiceOptions.DEFAULT_NESSIE_PORT)
                    .withName("nessie")
                    .withProtocol("TCP")
                    .build());
    if (nessie.getSpec().remoteDebug().enabled()) {
      containerBuilder.addToPorts(
          new ContainerPortBuilder()
              .withContainerPort(nessie.getSpec().remoteDebug().port())
              .withName("debug")
              .withProtocol("TCP")
              .build());
    }
    return containerBuilder.build();
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
    container.getVolumeMounts().add(volumeMount("rocks-storage", "/rocks-nessie"));
    // Note: readOnly: false creates an infinite reconcile loop, because the actual deployment
    // will contain readOnly: null regardless of the value in the desired deployment.
    PersistentVolumeClaimVolumeSource claim =
        new PersistentVolumeClaimVolumeSource(nessie.getMetadata().getName(), null);
    volumes.add(
        new VolumeBuilder().withName("rocks-storage").withPersistentVolumeClaim(claim).build());
  }

  private static void configureJdbc(Nessie nessie, Container container) {
    JdbcOptions jdbc = nessie.getSpec().versionStore().jdbc();
    if (jdbc != null && jdbc.credentials() != null) {
      container
          .getEnv()
          .add(
              envVarFromSecret(
                  "QUARKUS_DATASOURCE_USERNAME",
                  jdbc.credentials().secretRef(),
                  jdbc.credentials().usernameKey()));
      container
          .getEnv()
          .add(
              envVarFromSecret(
                  "QUARKUS_DATASOURCE_PASSWORD",
                  jdbc.credentials().secretRef(),
                  jdbc.credentials().passwordKey()));
    }
  }

  private static void configureBigTable(Nessie nessie, Container container, List<Volume> volumes) {
    BigTableOptions bigTable = nessie.getSpec().versionStore().bigTable();
    if (bigTable != null && bigTable.credentials() != null) {
      container.getVolumeMounts().add(volumeMount("bigtable-creds", "/bigtable-nessie"));
      volumes.add(
          new VolumeBuilder()
              .withName("bigtable-creds")
              .withSecret(
                  new SecretVolumeSourceBuilder()
                      .withSecretName(bigTable.credentials().secretRef().getName())
                      .withItems(
                          new KeyToPathBuilder()
                              .withKey(bigTable.credentials().serviceAccountKey())
                              .withPath("sa_credentials.json")
                              .build())
                      .build())
              .build());
    }
  }

  private static void configureMongo(Nessie nessie, Container container) {
    MongoDbOptions mongoDb = nessie.getSpec().versionStore().mongoDb();
    if (mongoDb != null && mongoDb.credentials() != null) {
      container
          .getEnv()
          .add(
              envVarFromSecret(
                  "QUARKUS_MONGODB_CREDENTIALS_USERNAME",
                  mongoDb.credentials().secretRef(),
                  mongoDb.credentials().usernameKey()));
      container
          .getEnv()
          .add(
              envVarFromSecret(
                  "QUARKUS_MONGODB_CREDENTIALS_PASSWORD",
                  mongoDb.credentials().secretRef(),
                  mongoDb.credentials().passwordKey()));
    }
  }

  private static void configureCassandra(Nessie nessie, Container container) {
    CassandraOptions cassandra = nessie.getSpec().versionStore().cassandra();
    if (cassandra != null && cassandra.credentials() != null) {
      container
          .getEnv()
          .add(
              envVarFromSecret(
                  "QUARKUS_CASSANDRA_AUTH_USERNAME",
                  cassandra.credentials().secretRef(),
                  cassandra.credentials().usernameKey()));
      container
          .getEnv()
          .add(
              envVarFromSecret(
                  "QUARKUS_CASSANDRA_AUTH_PASSWORD",
                  cassandra.credentials().secretRef(),
                  cassandra.credentials().passwordKey()));
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
                  "AWS_ACCESS_KEY_ID", credentials.secretRef(), credentials.awsAccessKeyId()));
      container
          .getEnv()
          .add(
              envVarFromSecret(
                  "AWS_SECRET_ACCESS_KEY",
                  credentials.secretRef(),
                  credentials.awsSecretAccessKey()));
    }
  }

  private static void configureExtraEnv(Nessie nessie, Deployment deployment) {
    if (nessie.getSpec().extraEnv() != null) {
      List<EnvVar> envVars =
          deployment.getSpec().getTemplate().getSpec().getContainers().get(0).getEnv();
      // Regular env vars (name-value pairs) were already handled by ConfigMapDependent
      nessie.getSpec().extraEnv().stream()
          .filter(e -> e.getValueFrom() != null)
          .forEach(envVars::add);
    }
  }

  private static EnvVar envVarFromSecret(String name, LocalObjectReference secretRef, String key) {
    return new EnvVarBuilder()
        .withName(name)
        .withValueFrom(
            new EnvVarSourceBuilder()
                .withSecretKeyRef(
                    new SecretKeySelectorBuilder()
                        .withName(secretRef.getName())
                        .withKey(key)
                        .build())
                .build())
        .build();
  }

  private static VolumeMount volumeMount(String name, String mountPath) {
    return new VolumeMountBuilder().withName(name).withMountPath(mountPath).build();
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
