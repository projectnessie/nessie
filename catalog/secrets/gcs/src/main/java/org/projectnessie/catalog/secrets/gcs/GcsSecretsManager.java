/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.catalog.secrets.gcs;

import static com.google.api.gax.rpc.StatusCode.Code.NOT_FOUND;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.google.api.core.ApiFuture;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode;
import com.google.cloud.secretmanager.v1.AccessSecretVersionRequest;
import com.google.cloud.secretmanager.v1.AccessSecretVersionResponse;
import com.google.cloud.secretmanager.v1.SecretManagerServiceClient;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import org.projectnessie.catalog.secrets.AbstractStringBasedSecretsManager;

public class GcsSecretsManager extends AbstractStringBasedSecretsManager {
  private final String prefix;
  private final SecretManagerServiceClient client;
  private final long getSecretTimeout;

  public GcsSecretsManager(
      SecretManagerServiceClient client, String prefix, Duration getSecretTimeout) {
    this.client = client;
    this.prefix = prefix;
    this.getSecretTimeout = getSecretTimeout.toMillis();
  }

  @Override
  protected String resolveSecretString(String name) {
    String secretId = nameToSecretId(name);

    ApiFuture<AccessSecretVersionResponse> responseCallable =
        client.accessSecretVersionCallable().futureCall(request(secretId));
    try {
      AccessSecretVersionResponse response = responseCallable.get(getSecretTimeout, MILLISECONDS);
      return response.hasPayload() ? secret(response) : null;
    } catch (ExecutionException e) {
      Throwable c = e.getCause();
      if (c instanceof ApiException) {
        ApiException apiException = (ApiException) c;
        StatusCode status = apiException.getStatusCode();
        if (status.getCode() == NOT_FOUND) {
          return null;
        }
      }
      // Let all other API exceptions bubble up.
      throw new RuntimeException(c);
    } catch (InterruptedException | TimeoutException e) {
      throw new RuntimeException(e);
    }
  }

  private String secret(AccessSecretVersionResponse response) {
    return response.getPayload().getData().toStringUtf8();
  }

  private AccessSecretVersionRequest request(String name) {
    return AccessSecretVersionRequest.newBuilder().setName(name).build();
  }

  private String nameToSecretId(String name) {
    return prefix + name;
  }
}
