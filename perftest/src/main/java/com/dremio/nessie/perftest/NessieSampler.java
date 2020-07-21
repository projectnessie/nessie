/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dremio.nessie.perftest;

import com.dremio.nessie.client.NessieClient;
import com.dremio.nessie.client.NessieClient.AuthType;
import com.dremio.nessie.client.rest.NessieExtendedClientErrorException;
import com.dremio.nessie.client.rest.NessieInternalServerException;
import com.dremio.nessie.client.rest.NessiePreconditionFailedException;
import com.dremio.nessie.json.ObjectMapperBuilder;
import com.dremio.nessie.model.Branch;
import com.dremio.nessie.model.ImmutableBranch;
import com.dremio.nessie.model.ImmutableTable;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.function.Supplier;
import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.protocol.java.sampler.AbstractJavaSamplerClient;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.testelement.TestElement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NessieSampler extends AbstractJavaSamplerClient implements Serializable {

  private static final Logger logger = LoggerFactory.getLogger(NessieSampler.class);

  private static final ObjectMapper OBJECT_MAPPER = ObjectMapperBuilder.createObjectMapper();
  private static final Joiner SLASH = Joiner.on("/");
  private static final String METHOD_TAG = "method";
  private static final String PATH_TAG = "path";
  private static final String BRANCH_TAG = "branch";
  private static final String BASE_BRANCH_TAG = "base_branch";
  private static final String TABLE_TAG = "table";

  public static Arguments getArguments(String method,
                                       String path,
                                       String branch,
                                       String baseBranch,
                                       String table) {
    Arguments defaultParameters = new Arguments();
    defaultParameters.addArgument(METHOD_TAG, method);
    defaultParameters.addArgument(PATH_TAG, path);
    defaultParameters.addArgument(BRANCH_TAG, branch);
    defaultParameters.addArgument(BASE_BRANCH_TAG, baseBranch);
    defaultParameters.addArgument(TABLE_TAG, table);
    return defaultParameters;

  }

  enum Method {
    CREATE_BRANCH,
    DELETE_BRANCH,
    COMMIT,
    MERGE
  }

  private ThreadLocal<NessieClient> client = new ThreadLocal<>();
  private String name;
  private Method method;
  private String path;
  private String branch;
  private String baseBranch;
  private String table;
  private ThreadLocal<Branch> commitId = new ThreadLocal<>();

  private synchronized NessieClient nessieClient() {
    if (client.get() == null) {
      client.set(new NessieClient(AuthType.BASIC, path, "admin_user", "test123"));
    }
    return client.get();
  }

  @Override
  public Arguments getDefaultParameters() {
    Arguments defaultParameters = new Arguments();
    defaultParameters.addArgument(METHOD_TAG, "test");
    defaultParameters.addArgument(PATH_TAG, "http://localhost:19120/api/v1");
    defaultParameters.addArgument(BRANCH_TAG, "master");
    defaultParameters.addArgument(BASE_BRANCH_TAG, "master");
    defaultParameters.addArgument(TABLE_TAG, "table");
    return defaultParameters;
  }

  private void fillSampler(NessieSampleResult sampleResult,
                           String commitId,
                           int retries,
                           boolean ok,
                           String message,
                           int retCode) {
    sampleResult.sampleEnd();
    sampleResult.setBranch(branch);
    sampleResult.setMethod(method.toString());
    sampleResult.setCommitId(commitId);
    sampleResult.setRetries(retries);
    sampleResult.setSuccessful(ok);
    sampleResult.setResponseMessage(message);
    sampleResult.setBytes(message.getBytes().length);
    sampleResult.setResponseCode(Integer.toString(retCode));
    try {
      sampleResult.setURL(new URL(SLASH.join(path, method.name())));
    } catch (MalformedURLException e) {
      //pass
    }
    sampleResult.setSampleLabel(SLASH.join(branch, baseBranch, retries, commitId));
  }

  private SampleResult handle(Supplier<Branch> supplier) {
    NessieSampleResult sampleResult = new NessieSampleResult();
    sampleResult.sampleStart();
    int retries = 0;
    try {
      Branch cBranch = null;
      while (retries < 10) {
        try {
          cBranch = supplier.get();
          break;
        } catch (NessiePreconditionFailedException e) {
          commitId.remove();
        }
        retries++;
      }
      if (cBranch != null) {
        String cBranchStr = "ok";
        fillSampler(sampleResult, cBranch.getId(), retries, true, cBranchStr, 200);
        commitId.set(cBranch);
      } else {
        throw new UnsupportedOperationException("failed with too many retries");
      }
    } catch (NessieExtendedClientErrorException | NessieInternalServerException e) {
      String errStr = e.getNessieError().statusMessage();
      fillSampler(sampleResult, "", retries, false, errStr, e.getNessieError().errorCode());
    } catch (Throwable t) {
      //logger.error("Request was not successfully processed", t);
      String msg = t.getMessage() == null ? "" : t.getMessage();
      fillSampler(sampleResult, "", retries, false, msg, 500);
    }
    return sampleResult;
  }


  @Override
  public SampleResult runTest(JavaSamplerContext javaSamplerContext) {
    switch (method) {
      case CREATE_BRANCH: {
        return handle(() -> nessieClient().createBranch(ImmutableBranch.builder()
                                                                       .name(branch)
                                                                       .id(baseBranch)
                                                                       .build()));
      }
      case DELETE_BRANCH:
        break;
      case COMMIT: {
        return handle(() -> {
          if (commitId.get() == null) {
            Branch cBranch = nessieClient().getBranch(branch);
            commitId.set(cBranch);
          }
          return nessieClient().commit(commitId.get(), ImmutableTable.builder()
                                                                     .id("name.space." + table)
                                                                     .name(table)
                                                                     .metadataLocation(
                                                                       "path_on_disk_" + table)
                                                                     .build()
          );
        });
      }
      case MERGE:
        break;
      default:
        throw new UnsupportedOperationException("Not a valid enum " + method);
    }
    return null;
  }

  @Override
  public void setupTest(JavaSamplerContext context) {
    method = Method.values()[context.getIntParameter(METHOD_TAG)];
    path = context.getParameter(PATH_TAG);
    branch = context.getParameter(BRANCH_TAG, "x");
    baseBranch = context.getParameter(BASE_BRANCH_TAG, "master");
    table = context.getParameter(TABLE_TAG, "y");
    name = context.getParameter(TestElement.NAME);
  }
}

