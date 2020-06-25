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
package com.dremio.nessie.serverless;

import com.amazonaws.serverless.proxy.jersey.JerseyLambdaContainerHandler;
import com.amazonaws.serverless.proxy.model.AwsProxyRequest;
import com.amazonaws.serverless.proxy.model.AwsProxyResponse;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaRuntime;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import com.dremio.nessie.server.RestServerV1;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.glassfish.jersey.server.ResourceConfig;

public class NessieLambdaHandler implements RequestStreamHandler  {
  private final ResourceConfig jerseyApplication;
  private final JerseyLambdaContainerHandler<AwsProxyRequest, AwsProxyResponse> handler;

  /**
   * instantiate a lambda listener for the nessie server.
   */
  public NessieLambdaHandler() {
    jerseyApplication = new RestServerV1();
    LambdaRuntime.getLogger().log("REST Server initialized\n");
    handler = JerseyLambdaContainerHandler.getAwsProxyHandler(jerseyApplication);
    LambdaRuntime.getLogger().log("lambda handler initialized\n");
  }

  @Override
  public void handleRequest(InputStream inputStream, OutputStream outputStream, Context context)
    throws IOException {
    handler.proxyStream(inputStream, outputStream, context);
  }
}
