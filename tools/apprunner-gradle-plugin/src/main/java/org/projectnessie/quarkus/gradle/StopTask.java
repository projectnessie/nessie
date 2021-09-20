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
import org.gradle.api.DefaultTask;
import org.gradle.api.tasks.Internal;
import org.gradle.api.tasks.TaskAction;

public class StopTask extends DefaultTask {
  @Internal private AutoCloseable application;

  final Map<String, String> restoreSystemProps = new HashMap<>();

  public StopTask() {
    // intentionally empty
  }

  @TaskAction
  public void stop() {

    if (application == null) {
      getLogger().debug("No application found.");
      return;
    }

    try {
      application.close();
      getLogger().info("Quarkus application stopped.");
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      application = null;

      // Restore System properties
      restoreSystemProps.forEach(
          (k, v) -> {
            if (v != null) {
              System.setProperty(k, v);
            } else {
              System.getProperties().remove(k);
            }
          });
    }
  }

  public AutoCloseable getApplication() {
    return application;
  }

  public void setQuarkusApplication(AutoCloseable quarkusApplication) {
    application = quarkusApplication;
  }
}
