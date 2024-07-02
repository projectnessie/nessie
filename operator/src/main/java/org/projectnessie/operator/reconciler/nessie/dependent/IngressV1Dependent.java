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

import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.networking.v1.Ingress;
import io.fabric8.kubernetes.api.model.networking.v1.IngressBuilder;
import io.fabric8.kubernetes.api.model.networking.v1.IngressLoadBalancerIngress;
import io.fabric8.kubernetes.api.model.networking.v1.IngressRuleBuilder;
import io.fabric8.kubernetes.api.model.networking.v1.IngressSpecBuilder;
import io.fabric8.kubernetes.api.model.networking.v1.IngressStatus;
import io.fabric8.kubernetes.api.model.networking.v1.IngressTLSBuilder;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.KubernetesDependent;
import java.util.List;
import org.projectnessie.operator.reconciler.KubernetesHelper;
import org.projectnessie.operator.reconciler.nessie.NessieReconciler;
import org.projectnessie.operator.reconciler.nessie.resource.Nessie;
import org.projectnessie.operator.reconciler.nessie.resource.options.IngressOptions.Rule;
import org.projectnessie.operator.reconciler.nessie.resource.options.IngressOptions.Tls;

@KubernetesDependent(labelSelector = NessieReconciler.DEPENDENT_RESOURCES_SELECTOR)
public class IngressV1Dependent extends AbstractIngressDependent<Ingress> {

  protected IngressV1Dependent() {
    super(Ingress.class);
  }

  @Override
  public Ingress desired(Nessie nessie, Context<Nessie> context) {
    KubernetesHelper helper = KubernetesHelper.retrieveFromContext(context);
    ObjectMeta metadata =
        helper
            .metaBuilder(nessie)
            .withAnnotations(nessie.getSpec().ingress().annotations())
            .build();
    Ingress ingress =
        new IngressBuilder()
            .withMetadata(metadata)
            .withSpec(
                new IngressSpecBuilder()
                    .withIngressClassName(nessie.getSpec().ingress().ingressClassName())
                    .build())
            .build();
    configureRules(ingress, nessie);
    configureTls(ingress, nessie);
    return ingress;
  }

  private void configureRules(Ingress ingress, Nessie nessie) {
    List<Rule> rules = nessie.getSpec().ingress().rules();
    for (Rule rule : rules) {
      IngressRuleBuilder ruleBuilder = new IngressRuleBuilder();
      ruleBuilder.withHost(rule.host());
      for (String path : rule.paths()) {
        ruleBuilder
            .withNewHttp()
            .withPaths()
            .addNewPath()
            .withPath(path)
            .withPathType("ImplementationSpecific")
            .withNewBackend()
            .withNewService()
            .withName(nessie.getMetadata().getName())
            .withNewPort()
            .withNumber(nessie.getSpec().service().port())
            .endPort()
            .endService()
            .endBackend()
            .endPath()
            .endHttp();
      }
      ingress.getSpec().getRules().add(ruleBuilder.build());
    }
  }

  private void configureTls(Ingress ingress, Nessie nessie) {
    for (Tls tls : nessie.getSpec().ingress().tls()) {
      IngressTLSBuilder tlsBuilder = new IngressTLSBuilder();
      tlsBuilder.withHosts(tls.hosts());
      tlsBuilder.withSecretName(tls.secret());
      ingress.getSpec().getTls().add(tlsBuilder.build());
    }
  }

  public static String getExposedUrl(Ingress ingress) {
    IngressLoadBalancerIngress ing = ingress.getStatus().getLoadBalancer().getIngress().get(0);
    return "https://" + (ing.getHostname() != null ? ing.getHostname() : ing.getIp());
  }

  public static void updateStatus(Nessie nessie, Context<Nessie> context) {
    context
        .getSecondaryResource(Ingress.class)
        .ifPresentOrElse(
            ingress -> nessie.getStatus().setExposedUrl(getExposedUrl(ingress)),
            () -> nessie.getStatus().setExposedUrl(null));
  }

  public static class ActivationCondition
      extends AbstractIngressDependent.ActivationCondition<Ingress> {

    public ActivationCondition() {
      super("v1");
    }
  }

  public static class ReadyCondition extends AbstractIngressDependent.ReadyCondition<Ingress> {

    public ReadyCondition() {
      super(Ingress.class);
    }

    @Override
    protected boolean checkIngressReady(Ingress ingress) {
      IngressStatus status = ingress.getStatus();
      if (status != null) {
        List<IngressLoadBalancerIngress> ingresses = status.getLoadBalancer().getIngress();
        return ingresses != null && !ingresses.isEmpty() && ingresses.get(0).getIp() != null;
      }
      return false;
    }
  }
}
