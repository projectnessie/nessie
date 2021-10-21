/*
 * Copyright (C) 2020 Dremio
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
package org.projectnessie.quarkus.gradle;

import java.util.HashMap;
import java.util.Map;
import org.gradle.api.Project;
import org.gradle.api.provider.MapProperty;
import org.gradle.api.provider.Property;

public class QuarkusAppExtension {
  private final MapProperty<String, Object> props;
  private final MapProperty<String, String> systemProperties;
  private final Property<String> nativeBuilderImage;

  private static Map<String, Object> defaultProps() {
    Map<String, Object> map = new HashMap<>();
    map.put("quarkus.http.test-port", 0);
    return map;
  }

  public QuarkusAppExtension(Project project) {
    props = project.getObjects().mapProperty(String.class, Object.class);
    props.set(defaultProps());

    systemProperties = project.getObjects().mapProperty(String.class, String.class);

    // This is not doing anything with Docker or building a native image, just a quirk of Quarkus
    // since 1.10.
    nativeBuilderImage =
        project
            .getObjects()
            .property(String.class)
            .convention("quay.io/quarkus/ubi-quarkus-native-image:21.0.0-java11");
  }

  public MapProperty<String, Object> getPropsProperty() {
    return props;
  }

  public MapProperty<String, String> getSystemProperties() {
    return systemProperties;
  }

  public Property<String> getNativeBuilderImageProperty() {
    return nativeBuilderImage;
  }

  @SuppressWarnings("unused") // convenience method
  public void setNativeBuilderImage(String nativeBuilderImage) {
    this.nativeBuilderImage.set(nativeBuilderImage);
  }
}
