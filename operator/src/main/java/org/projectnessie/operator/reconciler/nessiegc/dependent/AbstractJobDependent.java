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
package org.projectnessie.operator.reconciler.nessiegc.dependent;

import static org.projectnessie.operator.reconciler.nessie.dependent.ServiceAccountDependent.serviceAccountName;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarSourceBuilder;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.PodSpec;
import io.fabric8.kubernetes.api.model.PodSpecBuilder;
import io.fabric8.kubernetes.api.model.PodTemplateSpec;
import io.fabric8.kubernetes.api.model.PodTemplateSpecBuilder;
import io.fabric8.kubernetes.api.model.batch.v1.JobSpec;
import io.fabric8.kubernetes.api.model.batch.v1.JobSpecBuilder;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.CRUDKubernetesDependentResource;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.projectnessie.operator.events.EventReason;
import org.projectnessie.operator.exception.InvalidSpecException;
import org.projectnessie.operator.reconciler.nessie.resource.options.ImageOptions;
import org.projectnessie.operator.reconciler.nessie.resource.options.WorkloadOptions;
import org.projectnessie.operator.reconciler.nessiegc.resource.NessieGc;
import org.projectnessie.operator.reconciler.nessiegc.resource.NessieGcSpec;
import org.projectnessie.operator.reconciler.nessiegc.resource.options.DatasourceOptions;
import org.projectnessie.operator.reconciler.nessiegc.resource.options.DatasourceOptions.DatasourceType;
import org.projectnessie.operator.reconciler.nessiegc.resource.options.IcebergOptions;
import org.projectnessie.operator.reconciler.nessiegc.resource.options.JdbcOptions;
import org.projectnessie.operator.reconciler.nessiegc.resource.options.MarkOptions;
import org.projectnessie.operator.reconciler.nessiegc.resource.options.SweepOptions;

public abstract class AbstractJobDependent<R extends HasMetadata>
    extends CRUDKubernetesDependentResource<R, NessieGc> {

  private static final int ONE_WEEK = 60 * 60 * 24 * 7;

  protected AbstractJobDependent(Class<R> resourceClass) {
    super(resourceClass);
  }

  protected JobSpec newJobSpec(NessieGc nessie) {
    return new JobSpecBuilder()
        .withTtlSecondsAfterFinished(ONE_WEEK)
        .withTemplate(newPodTemplateSpec(nessie))
        .build();
  }

  protected PodTemplateSpec newPodTemplateSpec(NessieGc nessieGc) {
    return new PodTemplateSpecBuilder()
        .withMetadata(
            new ObjectMetaBuilder()
                .addToLabels(nessieGc.getSpec().job().labels())
                .withAnnotations(nessieGc.getSpec().job().annotations())
                .build())
        .withSpec(newPodSpec(nessieGc))
        .build();
  }

  protected PodSpec newPodSpec(NessieGc nessieGc) {
    WorkloadOptions pod = nessieGc.getSpec().job();
    return new PodSpecBuilder()
        .withServiceAccountName(serviceAccountName(nessieGc, pod.serviceAccount()))
        .withSecurityContext(pod.podSecurityContext())
        .withImagePullSecrets(
            pod.image().pullSecretRef() != null ? List.of(pod.image().pullSecretRef()) : List.of())
        .withNodeSelector(pod.nodeSelector())
        .withTolerations(pod.tolerations())
        .withAffinity(pod.affinity())
        .withRestartPolicy("OnFailure")
        .withContainers(newContainer(nessieGc, pod))
        .build();
  }

  protected Container newContainer(NessieGc nessieGc, WorkloadOptions pod) {
    return new ContainerBuilder()
        .withName("nessie-gc")
        .withImage(pod.image().fullName(ImageOptions.DEFAULT_NESSIE_GC_REPOSITORY))
        .withImagePullPolicy(Objects.requireNonNull(pod.image().pullPolicy()).name())
        .withResources(pod.resources())
        .withSecurityContext(pod.containerSecurityContext())
        .withEnv(createEnv(nessieGc))
        .withArgs(createArgs(nessieGc))
        .build();
  }

  private List<EnvVar> createEnv(NessieGc nessieGc) {
    List<EnvVar> env = new ArrayList<>();
    JdbcOptions jdbc = nessieGc.getSpec().datasource().jdbc();
    if (jdbc != null) {
      if (jdbc.username() != null) {
        env.add(new EnvVar("JDBC_USERNAME", jdbc.username(), null));
      }
      if (jdbc.password() != null) {
        env.add(
            new EnvVar(
                "JDBC_PASSWORD",
                null,
                new EnvVarSourceBuilder()
                    .withNewSecretKeyRef(jdbc.password().secret(), jdbc.password().key(), false)
                    .build()));
      }
    }
    return env;
  }

  private List<String> createArgs(NessieGc nessieGc) {
    List<String> args = new ArrayList<>();
    args.add("gc");
    configureNessie(nessieGc, args);
    configureDatasource(nessieGc.getSpec(), args);
    configureMarkPhase(nessieGc.getSpec(), args);
    configureSweepPhase(nessieGc.getSpec(), args);
    configureIceberg(nessieGc.getSpec(), args);
    return args;
  }

  @SuppressWarnings("HttpUrlsUsage")
  private static void configureNessie(NessieGc nessieGc, List<String> args) {
    String nessieName = nessieGc.getSpec().nessieRef().getName();
    args.add("--uri");
    args.add("http://%s:19120/api/v2".formatted(nessieName));
  }

  private static void configureDatasource(NessieGcSpec spec, List<String> args) {
    DatasourceOptions datasourceOptions = spec.datasource();
    switch (datasourceOptions.type()) {
      case InMemory -> args.add("--inmemory");
      case Jdbc -> {
        args.add("--jdbc");
        args.add("--jdbc-url");
        args.add(Objects.requireNonNull(spec.datasource().jdbc()).url());
        if (spec.datasource().jdbc().username() != null) {
          args.add("--jdbc-username");
          args.add("$(JDBC_USERNAME)");
        }
        if (spec.datasource().jdbc().password() != null) {
          args.add("--jdbc-password");
          args.add("$(JDBC_PASSWORD)");
        }
        if (spec.datasource().jdbc().createSchema()) {
          args.add("--jdbc-schema");
          args.add("CREATE_IF_NOT_EXISTS");
        }
      }
      default ->
          throw new IllegalStateException(
              "Unexpected datasource type: " + datasourceOptions.type());
    }
  }

  private static void configureMarkPhase(NessieGcSpec spec, List<String> args) {
    MarkOptions mark = spec.mark();
    args.add("--identify-parallelism");
    args.add(String.valueOf(mark.parallelism()));
    args.add("--default-cutoff");
    args.add(mark.defaultCutoffPolicy());
    mark.cutoffPolicies()
        .forEach(
            (k, v) -> {
              args.add("--cutoff");
              args.add(k + "=" + v);
            });
  }

  private static void configureSweepPhase(NessieGcSpec spec, List<String> args) {
    SweepOptions sweep = spec.sweep();
    args.add("--expiry-parallelism");
    args.add(String.valueOf(sweep.parallelism()));
    if (sweep.deferDeletes()) {
      if (spec.datasource().type() == DatasourceType.InMemory) {
        throw new InvalidSpecException(
            EventReason.InvalidGcConfig,
            "In-memory datasource not allowed when deferred deletes are enabled");
      }
      args.add("--defer-deletes");
    } else {
      args.add("--no-defer-deletes");
    }
  }

  private static void configureIceberg(NessieGcSpec spec, List<String> args) {
    IcebergOptions iceberg = spec.iceberg();
    iceberg
        .icebergProperties()
        .forEach(
            (k, v) -> {
              args.add("--iceberg");
              args.add(k + "=" + v);
            });
    iceberg
        .hadoopConf()
        .forEach(
            (k, v) -> {
              args.add("--hadoop");
              args.add(k + "=" + v);
            });
  }
}
