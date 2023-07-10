/*
 * Copyright (C) 2023 Dremio
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
package org.projectnessie.catalog.server;

import io.quarkus.runtime.Startup;
import java.util.Map;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Disposes;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.iceberg.hadoop.HadoopConfigurable;
import org.apache.iceberg.io.FileIO;

@ApplicationScoped
public class FileIOProvider {
  @Inject NessieIcebergRestConfig config;

  @Produces
  @Singleton
  @Startup
  public FileIO produceFileIO() {
    String fileIoImplementation = config.fileIoImplementation();
    Map<String, String> properties = config.fileIoConfig();

    try {
      FileIO fileIO =
          Class.forName(fileIoImplementation)
              .asSubclass(FileIO.class)
              .getDeclaredConstructor()
              .newInstance();
      if (fileIO instanceof HadoopConfigurable) {
        HadoopConfigurable cfg = (HadoopConfigurable) fileIO;
        cfg.setConf(config.hadoopConf());
      }
      fileIO.initialize(properties);
      return fileIO;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void disposeFileIO(@Disposes FileIO fileIO) {
    fileIO.close();
  }
}
