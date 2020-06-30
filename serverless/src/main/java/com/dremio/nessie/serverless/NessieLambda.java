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

import com.amazonaws.serverless.proxy.internal.LambdaContainerHandler;
import com.amazonaws.serverless.proxy.jersey.JerseyLambdaContainerHandler;
import com.amazonaws.serverless.proxy.model.AwsProxyRequest;
import com.amazonaws.serverless.proxy.model.AwsProxyResponse;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import com.dremio.nessie.server.RestServerV1;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.glassfish.jersey.server.ResourceConfig;

public class NessieLambda implements RequestStreamHandler {
  private static final ResourceConfig JERSEY_APPLICATION = new RestServerV1();
  private static final JerseyLambdaContainerHandler<AwsProxyRequest, AwsProxyResponse> HANDLER
      = JerseyLambdaContainerHandler.getAwsProxyHandler(JERSEY_APPLICATION);

  static {
    LambdaContainerHandler.getContainerConfig().setServiceBasePath("api/v1");
    LambdaContainerHandler.getContainerConfig().setStripBasePath(true);
  }

  @Override
  public void handleRequest(InputStream inputStream, OutputStream outputStream, Context context)
      throws IOException {
    HANDLER.proxyStream(inputStream, outputStream, context);
  }
}
