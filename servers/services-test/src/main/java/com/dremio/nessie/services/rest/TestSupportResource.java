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

package com.dremio.nessie.services.rest;

import java.security.Principal;

import javax.annotation.Priority;
import javax.enterprise.context.RequestScoped;
import javax.enterprise.inject.Alternative;
import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.nessie.api.TestSupportApi;
import com.dremio.nessie.error.NessieUnsupportedOperationException;
import com.dremio.nessie.model.CommitMeta;
import com.dremio.nessie.model.Contents;
import com.dremio.nessie.services.config.ServerConfig;
import com.dremio.nessie.versioned.VersionStore;

/**
 * REST endpoint to retrieve server settings.
 */
@RequestScoped
@Alternative
@Priority(javax.interceptor.Interceptor.Priority.APPLICATION)
public class TestSupportResource extends BaseResource implements TestSupportApi {

  private static final Logger logger = LoggerFactory.getLogger(TestSupportResource.class);

  @Inject
  public TestSupportResource(ServerConfig config, Principal principal,
                             VersionStore<Contents, CommitMeta> store) {
    super(config, principal, store);
  }

  @Override
  public void resetStoreUnsafe() throws NessieUnsupportedOperationException {
    try {
      getStore().resetStoreUnsafe(getConfig().getDefaultBranch());
    } catch (UnsupportedOperationException e) {
      throw new NessieUnsupportedOperationException("Operation not implemented");
    }
  }
}
