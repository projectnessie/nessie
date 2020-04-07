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
package com.dremio.iceberg.serverless;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.glassfish.jersey.server.ResourceConfig;

import com.amazonaws.serverless.proxy.jersey.JerseyLambdaContainerHandler;
import com.amazonaws.serverless.proxy.model.AwsProxyRequest;
import com.amazonaws.serverless.proxy.model.AwsProxyResponse;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import com.dremio.iceberg.server.RestServerV1;

public class IcebergAlleyLambda implements RequestStreamHandler {
  private static final ResourceConfig jerseyApplication = new RestServerV1();
  private static final JerseyLambdaContainerHandler<AwsProxyRequest, AwsProxyResponse> handler
    = JerseyLambdaContainerHandler.getAwsProxyHandler(jerseyApplication);
  // If you are using HTTP APIs with the version 2.0 of the proxy model, use the getHttpApiV2ProxyHandler
  // method:
  // JerseyLambdaContainerHandler<HttpApiV2ProxyRequest, AwsProxyResponse> handler =
  //        JerseyLambdaContainerHandler.getHttpApiV2ProxyHandler(jerseyApplication);

  @Override
  public void handleRequest(InputStream inputStream, OutputStream outputStream, Context context)
    throws IOException {
    handler.proxyStream(inputStream, outputStream, context);
  }
}
