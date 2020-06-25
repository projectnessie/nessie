/*
 * Copyright 2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance
 * with the License. A copy of the License is located at
 *
 * http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES
 * OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */
package com.amazonaws.serverless.proxy;

import com.amazonaws.serverless.exceptions.InvalidRequestEventException;
import com.amazonaws.serverless.proxy.internal.LambdaContainerHandler;
import com.amazonaws.serverless.proxy.model.AwsProxyResponse;
import com.amazonaws.serverless.proxy.model.ErrorModel;
import com.amazonaws.serverless.proxy.model.Headers;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.IOException;
import java.io.OutputStream;
import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation of the <code>ExceptionHandler</code> object that returns AwsProxyResponse
 * objects.
 * <p>
 * Returns application/json messages with a status code of 500 when the RequestReader failed to read
 * the incoming event or if InternalServerErrorException is thrown. For all other exceptions returns
 * a 502. Responses are populated with a JSON object containing a message property.
 *
 * @see ExceptionHandler
 */
public class AwsProxyExceptionHandler
  implements ExceptionHandler<AwsProxyResponse> {

  static final String INTERNAL_SERVER_ERROR = "Internal Server Error";

  //-------------------------------------------------------------
  // Constants
  //-------------------------------------------------------------
  static final String GATEWAY_TIMEOUT_ERROR = "Gateway timeout";
  private static final Headers headers = new Headers();

  //-------------------------------------------------------------
  // Variables - Private - Static
  //-------------------------------------------------------------

  static {
    headers.putSingle(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON);
  }

  //-------------------------------------------------------------
  // Constructors
  //-------------------------------------------------------------

  private final Logger log = LoggerFactory.getLogger(com.amazonaws.serverless.proxy.AwsProxyExceptionHandler.class);

  //-------------------------------------------------------------
  // Implementation - ExceptionHandler
  //-------------------------------------------------------------

  @Override
  public AwsProxyResponse handle(Throwable ex) {
    log.error("Called exception handler for:", ex);

    // adding a print stack trace in case we have no appender or we are running inside SAM local, where need the
    // output to go to the stderr.
    ex.printStackTrace();
    if (ex instanceof InvalidRequestEventException || ex instanceof InternalServerErrorException) {
      return new AwsProxyResponse(500, headers, getErrorJson(INTERNAL_SERVER_ERROR));
    } else {
      return new AwsProxyResponse(502, headers, getErrorJson(GATEWAY_TIMEOUT_ERROR));
    }
  }


  @Override
  public void handle(Throwable ex, OutputStream stream) throws IOException {
    AwsProxyResponse response = handle(ex);

    LambdaContainerHandler.getObjectMapper().writeValue(stream, response);
  }

  //-------------------------------------------------------------
  // Methods - Protected
  //-------------------------------------------------------------

  String getErrorJson(String message) {

    try {
      return LambdaContainerHandler.getObjectMapper().writeValueAsString(new ErrorModel(message));
    } catch (JsonProcessingException e) {
      log.error("Could not produce error JSON", e);
      return "{ \"message\": \"" + message + "\" }";
    }
  }
}
