/*
 * Copyright (C) 2022 Dremio
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

plugins {
  `java-platform`
  `maven-publish`
  signing
  id("org.projectnessie.nessie-project")
}

extra["maven.name"] = "Nessie - Bill of Materials (BOM)"

dependencies {
  constraints {
    api(rootProject)
    api(projects.clients)
    api(projects.clients.client)
    api(projects.clients.deltalake)
    api(projects.clients.icebergViews)
    api(projects.clients.spark31Extensions)
    api(projects.clients.spark32Extensions)
    api(projects.clients.sparkAntlrGrammar)
    api(projects.clients.sparkExtensionsBase)
    api(projects.compatibility)
    api(projects.compatibility.common)
    api(projects.compatibility.compatibilityTests)
    api(projects.compatibility.jersey)
    api(projects.gc)
    api(projects.gc.gcBase)
    api(projects.model)
    api(projects.perftest)
    api(projects.servers)
    api(projects.servers.jaxRs)
    api(projects.servers.jaxRsTestextension)
    api(projects.servers.jaxRsTests)
    api(projects.servers.quarkusCommon)
    api(projects.servers.quarkusCli)
    api(projects.servers.quarkusServer)
    api(projects.servers.quarkusTests)
    api(projects.servers.restServices)
    api(projects.servers.services)
    api(projects.servers.store)
    api(projects.servers.storeProto)
    api(projects.tools)
    api(projects.tools.contentGenerator)
    api(projects.ui)
    api(projects.versioned)
    api(projects.versioned.persist)
    api(projects.versioned.persist.adapter)
    api(projects.versioned.persist.bench)
    api(projects.versioned.persist.dynamodb)
    api(projects.versioned.persist.dynamodb) { testJarCapability() }
    api(projects.versioned.persist.inmem)
    api(projects.versioned.persist.inmem) { testJarCapability() }
    api(projects.versioned.persist.mongodb)
    api(projects.versioned.persist.mongodb) { testJarCapability() }
    api(projects.versioned.persist.nontx)
    api(projects.versioned.persist.rocks)
    api(projects.versioned.persist.rocks) { testJarCapability() }
    api(projects.versioned.persist.serialize)
    api(projects.versioned.persist.serializeProto)
    api(projects.versioned.persist.persistStore)
    api(projects.versioned.persist.persistTests)
    api(projects.versioned.persist.tx)
    api(projects.versioned.persist.tx) { testJarCapability() }
    api(projects.versioned.spi)
    api(projects.versioned.tests)
  }
}

javaPlatform { allowDependencies() }
