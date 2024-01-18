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
package org.projectnessie.operator.reconciler.nessiegc.resource;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.fabric8.kubernetes.api.model.Namespaced;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.model.annotation.Group;
import io.fabric8.kubernetes.model.annotation.Version;
import io.sundr.builder.annotations.Buildable;
import io.sundr.builder.annotations.BuildableReference;
import org.projectnessie.operator.utils.ResourceUtils;

@Version(NessieGc.VERSION)
@Group(NessieGc.GROUP)
@Buildable(
    builderPackage = "io.fabric8.kubernetes.api.builder",
    editableEnabled = false,
    refs = {
      @BuildableReference(ObjectMeta.class),
      @BuildableReference(CustomResource.class),
    })
public class NessieGc extends CustomResource<NessieGcSpec, NessieGcStatus> implements Namespaced {

  public static final String GROUP = "nessie.projectnessie.org";
  public static final String VERSION = "v1alpha1";
  public static final String KIND = "NessieGc";

  public void validate() {
    ResourceUtils.validateName(getMetadata().getName());
    getSpec().validate();
  }

  @JsonIgnore
  public NessieGcBuilder edit() {
    return new NessieGcBuilder(this);
  }
}
