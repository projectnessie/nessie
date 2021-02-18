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
package org.projectnessie.perftest;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.protocol.java.sampler.AbstractJavaSamplerClient;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;
import org.projectnessie.client.NessieClient;
import org.projectnessie.client.rest.NessieServiceException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.ContentsKey;
import org.projectnessie.model.IcebergTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  private NessieClient client;
  private String path;
  private String branch;
  private String baseBranch;
  private ThreadLocal<Branch> commitId = new ThreadLocal<>();

  /**
   * Create a threadlocal nessie client. Multiple threads are run in the same JVM and they each get a client.
   */
  private synchronized NessieClient nessieClient() {
    if (client == null) {
      client = NessieClient.builder().withPath(path).withUsername("admin_user").withPassword("test123").build();
      try {
        client.getTreeApi().createReference(Branch.of("main", null));
      } catch (Exception t) {
        //pass - likely already created main
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
    defaultParameters.addArgument(BRANCH_TAG, "main");
    defaultParameters.addArgument(BASE_BRANCH_TAG, "main");
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
  private SampleResult handle(SupplierIO<Branch> supplier, Method method) {
    SampleResult sampleResult = new SampleResult();
    sampleResult.sampleStart();
    int retries = 0;
    try {
      Branch branch = null;
      while (retries < 10) {
        try {
          branch = supplier.get();
          break;
        } catch (IOException e) {
          commitId.remove();
        }
        retries++;
      }
      if (branch != null) {
        String branchStr = "ok";
        fillSampler(sampleResult, branch.getHash(), retries, true, branchStr, 200, method);
        commitId.set(branch);
      } else {
        throw new UnsupportedOperationException("failed with too many retries");
      }
    } catch (NessieServiceException e) {
      logger.warn("Request was not successfully processed", e);
      String errStr = e.getMessage();
      fillSampler(sampleResult, "", retries, false, errStr, e.getError().getStatus(), method);
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
        return handle(() -> {
          nessieClient().getTreeApi().createReference(Branch.of(branch, baseBranch));
          return (Branch) nessieClient().getTreeApi().getReferenceByName(branch);
        }, method);
      }
      case DELETE_BRANCH:
        break;
      case COMMIT: {
        return handle(() -> {
          if (commitId.get() == null) {
            Branch branch = (Branch) nessieClient().getTreeApi().getReferenceByName(this.branch);
            commitId.set(branch);
          }
          nessieClient().getContentsApi().setContents(ContentsKey.of("name", "space", table),
              commitId.get().getName(), commitId.get().getHash(),
              "test", IcebergTable.of("path_on_disk_" + table));
          //TODO: this test shouldn't be doing a get branch operation since that isn't required to complete a commit.
          return (Branch) nessieClient().getTreeApi().getReferenceByName(branch);
        }, method);
      }
      case MERGE:
        break;
      default:
        throw new UnsupportedOperationException("Not a valid enum " + method);
    }
    return null;
  }

  public interface SupplierIO<T> {
    T get() throws IOException;
  }

  @Override
  public void setupTest(JavaSamplerContext context) {
    path = context.getParameter(PATH_TAG);
    branch = context.getParameter(BRANCH_TAG, "x");
    baseBranch = context.getParameter(BASE_BRANCH_TAG, "main");
  }
}
