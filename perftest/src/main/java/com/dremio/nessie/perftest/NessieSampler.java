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
package com.dremio.nessie.perftest;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.function.Supplier;

import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.protocol.java.sampler.AbstractJavaSamplerClient;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.nessie.client.NessieService;
import com.dremio.nessie.client.NessieService.AuthType;
import com.dremio.nessie.client.rest.NessieExtendedClientErrorException;
import com.dremio.nessie.client.rest.NessieInternalServerException;
import com.dremio.nessie.client.rest.NessiePreconditionFailedException;
import com.dremio.nessie.model.Branch;
import com.dremio.nessie.model.ImmutableBranch;
import com.dremio.nessie.model.ImmutableTable;
import com.google.common.base.Joiner;

/**
 * NessieSampler creates REST calls to the Nessie service.
 *
 * <p>
 * This implements the interface required to create custom JMeter plugins in Java. It defines the arguments required to create a REST call
 * to Nessie via the Nessie client. Result objects are populated based on the return value of the Nessie REST call.
 * </p>
 */
public class NessieSampler extends AbstractJavaSamplerClient {
  private static final Logger logger = LoggerFactory.getLogger(NessieSampler.class);
  private static final Joiner SLASH = Joiner.on("/");
  private static final String METHOD_TAG = "method";
  private static final String PATH_TAG = "path";
  private static final String BRANCH_TAG = "branch";
  private static final String BASE_BRANCH_TAG = "base_branch";
  private static final String TABLE_TAG = "table";

  enum Method {
    CREATE_BRANCH,
    DELETE_BRANCH,
    COMMIT,
    MERGE
  }

  private NessieService client;
  private String path;
  private String branch;
  private String baseBranch;
  private ThreadLocal<Branch> commitId = new ThreadLocal<>();

  /**
   * Create a threadlocal nessie client. Multiple threads are run in the same JVM and they each get a client.
   */
  private synchronized NessieService nessieClient() {
    if (client == null) {
      client = new NessieService(AuthType.BASIC, path, "admin_user", "test123");
      try {
        client.createBranch(ImmutableBranch.builder().name("master").build());
      } catch (Exception t) {
        //pass - likely already created master
      }
    }
    return client;
  }

  /**
   * arguments and default values that JMeter test should set per sample.
   */
  @Override
  public Arguments getDefaultParameters() {
    Arguments defaultParameters = new Arguments();
    defaultParameters.addArgument(METHOD_TAG, "test");
    defaultParameters.addArgument(PATH_TAG, "testurl");
    defaultParameters.addArgument(BRANCH_TAG, "master");
    defaultParameters.addArgument(BASE_BRANCH_TAG, "master");
    defaultParameters.addArgument(TABLE_TAG, "table");
    return defaultParameters;
  }

  /**
   * construct a sample from the result of a call to Nessie API.
   */
  private void fillSampler(SampleResult sampleResult,
                           String commitId,
                           int retries,
                           boolean ok,
                           String message,
                           int retCode,
                           Method method) {
    sampleResult.sampleEnd();
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

  /**
   * delegate method which captures the result of a Nessie client call and returns a sample object.
   */
  private SampleResult handle(Supplier<Branch> supplier, Method method) {
    SampleResult sampleResult = new SampleResult();
    sampleResult.sampleStart();
    int retries = 0;
    try {
      Branch branch = null;
      while (retries < 10) {
        try {
          branch = supplier.get();
          break;
        } catch (NessiePreconditionFailedException e) {
          commitId.remove();
        }
        retries++;
      }
      if (branch != null) {
        String branchStr = "ok";
        fillSampler(sampleResult, branch.getId(), retries, true, branchStr, 200, method);
        commitId.set(branch);
      } else {
        throw new UnsupportedOperationException("failed with too many retries");
      }
    } catch (NessieExtendedClientErrorException | NessieInternalServerException e) {
      logger.warn("Request was not successfully processed", e);
      String errStr = e.getNessieError().statusMessage();
      fillSampler(sampleResult, "", retries, false, errStr, e.getNessieError().errorCode(), method);
    } catch (Throwable t) {
      logger.warn("Request was not successfully processed", t);
      String msg = t.getMessage() == null ? "" : t.getMessage();
      fillSampler(sampleResult, "", retries, false, msg, 500, method);
    }
    return sampleResult;
  }


  /**
   * generate a rest call based on input parameters for a single Nessie perf test sample, then execute and generate a sample result.
   */
  @Override
  public SampleResult runTest(JavaSamplerContext javaSamplerContext) {
    Method method = Method.values()[javaSamplerContext.getIntParameter(METHOD_TAG)];
    String table = javaSamplerContext.getParameter(TABLE_TAG, "y");
    switch (method) {
      case CREATE_BRANCH: {
        return handle(() -> nessieClient().createBranch(ImmutableBranch.builder()
                                                                       .name(branch)
                                                                       .id(baseBranch)
                                                                       .build()), method);
      }
      case DELETE_BRANCH:
        break;
      case COMMIT: {
        return handle(() -> {
          if (commitId.get() == null) {
            Branch branch = nessieClient().getBranch(this.branch);
            commitId.set(branch);
          }
          nessieClient().commit(commitId.get(), ImmutableTable.builder()
                                                               .id("name.space." + table)
                                                               .name(table)
                                                               .metadataLocation("path_on_disk_" + table)
                                                               .build()
          );
          return nessieClient().getBranch(branch);
        }, method);
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
    path = context.getParameter(PATH_TAG);
    branch = context.getParameter(BRANCH_TAG, "x");
    baseBranch = context.getParameter(BASE_BRANCH_TAG, "master");
  }
}

