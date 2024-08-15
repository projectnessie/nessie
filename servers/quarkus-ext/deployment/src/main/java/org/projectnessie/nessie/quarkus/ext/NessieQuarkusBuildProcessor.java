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
package org.projectnessie.nessie.quarkus.ext;

import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.builditem.nativeimage.ReflectiveHierarchyIgnoreWarningBuildItem;
import org.jboss.jandex.DotName;

public class NessieQuarkusBuildProcessor {

  @BuildStep
  ReflectiveHierarchyIgnoreWarningBuildItem ignoreReflectiveHierarchyWarnings() {

    // The relocated protobuf is a shadow jar and adding it to Quarkus' `quarkus.index-dependency.`
    // doesn't work.
    DotName protobufRelocated = DotName.createSimple("org.projectnessie.nessie.relocated.protobuf");
    // Quarkus complains about SecretKeySpec - but that's in the Java run time (quite hard to index
    // that)
    DotName javaxCryptoSpec = DotName.createSimple("javax.crypto.spec");

    return new ReflectiveHierarchyIgnoreWarningBuildItem(
        name ->
            name.packagePrefixName().equals(protobufRelocated)
                || name.packagePrefixName().equals(javaxCryptoSpec));
  }
}
