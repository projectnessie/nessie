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

package com.dremio.nessie.serverless;

import com.amazonaws.services.lambda.runtime.ClientContext;
import com.amazonaws.services.lambda.runtime.CognitoIdentity;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.LambdaRuntime;
import com.dremio.nessie.json.ObjectMapperContextResolver;
import com.google.common.base.MoreObjects;
import com.google.common.base.Throwables;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.Date;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import org.glassfish.jersey.client.JerseyClient;
import org.glassfish.jersey.client.JerseyClientBuilder;

public class NessieLambda {
  static {
    System.setProperty("io.netty.tryReflectionSetAccessible", String.valueOf(true));
  }

  private final Client client;
  private final String endpoint;
  private NessieLambdaHandler handler;

  /**
   * instantiate a lambda listener for the nessie server.
   */
  public NessieLambda() {
    LambdaRuntime.getLogger().log("Starting initialization\n");
    JerseyClientBuilder builder = (JerseyClientBuilder) JerseyClientBuilder.newBuilder();
    JerseyClient client = builder.register(ObjectMapperContextResolver.class).build();
    this.client = client;
    LambdaRuntime.getLogger().log("Client initialized\n");
    String runtimeApiHost = System.getenv("AWS_LAMBDA_RUNTIME_API");
    endpoint = "http://" + runtimeApiHost + "/2018-06-01/runtime";
    try {
      handler = new NessieLambdaHandler();
    } catch (Throwable t) {
      LambdaRuntime.getLogger().log("Failed to initialize: " + Throwables.getStackTraceAsString(t) + "\n");
      client("init/error").request()
                          .header("Lambda-Runtime-Function-Error-Type", "Unhandled")
                          .post(
                            Entity.entity(toJson(t), MediaType.APPLICATION_JSON_TYPE));
      throw t;
    }
  }

  private WebTarget client(String path) {
    return client.target(endpoint).path(path);
  }

  /**
   * run the service.
   */
  @SuppressWarnings("VariableDeclarationUsageDistance")
  public void run() {
    while (true) {
      LambdaRuntime.getLogger().log("Waiting for next invocation.\n");
      Response response = client("invocation/next").request().get();
      LambdaRuntime.getLogger().log("Next invocation received.\n");
      if (response.getStatus() == 200) {
        String reqId = null;
        try {
          InputStream is = response.readEntity(InputStream.class);
          ByteArrayOutputStream os = new ByteArrayOutputStream();
          LambdaRuntime.getLogger().log("Building context.\n");
          HeadersContext context = new HeadersContext(response.getHeaders());
          reqId = context.getAwsRequestId();
          LambdaRuntime.getLogger().log(context.toString());
          handler.handleRequest(is, os, context);
          LambdaRuntime.getLogger().log("Request Completed.\n");
          client("invocation/" + context.getAwsRequestId() + "/response")
              .request(MediaType.APPLICATION_JSON_TYPE)
              .header("_X_AMZN_TRACE_ID", response.getHeaders().getFirst("_X_AMZN_TRACE_ID"))
              .post(Entity.entity(os.toByteArray(), MediaType.APPLICATION_JSON_TYPE));
          LambdaRuntime.getLogger().log("Posted Response.\n");
        } catch (Throwable t) {
          LambdaRuntime.getLogger().log("Failed! " + Throwables.getStackTraceAsString(t) + "\n");
          if (reqId != null) {
            client("invocation/" + reqId + "/error")
                .request()
                .header("_X_AMZN_TRACE_ID", response.getHeaders().getFirst("_X_AMZN_TRACE_ID"))
                .post(Entity.entity(toJson(t), MediaType.APPLICATION_JSON_TYPE));
          }
        }
      }
    }
  }

  public static void main(String[] args) {
    new NessieLambda().run();
  }

  private String toJson(Throwable t) {
    StringBuilder builder = new StringBuilder();
    builder.append("{\"errorMessage\":\"");
    builder.append(Throwables.getStackTraceAsString(t));
    builder.append("\", \"errorType\":\"");
    builder.append(t.getMessage());
    builder.append("\"}");
    return builder.toString();
  }

  public static class HeadersContext implements Context {

    private static final String FUNCTION_NAME = System.getenv("AWS_LAMBDA_FUNCTION_NAME");
    private static final String FUNCTION_VERSION = System.getenv("AWS_LAMBDA_FUNCTION_VERSION");
    private static final String LOG_GROUP = System.getenv("AWS_LAMBDA_LOG_GROUP_NAME");
    private static final String LOG_STREAM = System.getenv("AWS_LAMBDA_LOG_STREAM_NAME");
    private static final String MEM_LIMIT = System.getenv("AWS_LAMBDA_FUNCTION_MEMORY_SIZE");
    private final MultivaluedMap<String, Object> headers;

    public HeadersContext(MultivaluedMap<String, Object> headers) {
      this.headers = headers;
    }


    @Override
    public String getAwsRequestId() {
      return (String) headers.getFirst("lambda-runtime-aws-request-id");
    }

    @Override
    public String getLogGroupName() {
      return LOG_GROUP;
    }

    @Override
    public String getLogStreamName() {
      return LOG_STREAM;
    }

    @Override
    public String getFunctionName() {
      return FUNCTION_NAME;
    }

    @Override
    public String getFunctionVersion() {
      return FUNCTION_VERSION;
    }

    @Override
    public String getInvokedFunctionArn() {
      return (String) headers.getFirst("lambda-runtime-invoked-function-arn");
    }


    /**
     * Try to return an identity for Cognito.
     */
    public CognitoIdentity getIdentity() {
      return new CognitoIdentity() {
        @Override
        public String getIdentityId() {
          return (String) headers.getFirst("lambda-runtime-cognito-identity");
        }

        @Override
        public String getIdentityPoolId() {
          getLogger().log("No known cognito identity pool id");
          return null;
        }
      };
    }

    @Override
    public ClientContext getClientContext() {
      return null;
    }

    @Override
    public int getRemainingTimeInMillis() {
      return (int) (new Date().toInstant().toEpochMilli()
                    - Long.parseUnsignedLong(
        (String) headers.getFirst("lambda-runtime-deadline-ms")));
    }

    @Override
    public int getMemoryLimitInMB() {
      return Integer.parseInt(MEM_LIMIT);
    }

    @Override
    public LambdaLogger getLogger() {
      return LambdaRuntime.getLogger();
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
                        .add("RequestId", getAwsRequestId())
                        .add("LogGroupName", getLogGroupName())
                        .add("LogStreamName", getLogStreamName())
                        .add("functionName", getFunctionName())
                        .add("functionVersion", getFunctionVersion())
                        .add("Arn", getInvokedFunctionArn())
                        .add("RemainingTime", getRemainingTimeInMillis())
                        .add("MemoryLimit", getMemoryLimitInMB())
                        .toString();
    }
  }

}
