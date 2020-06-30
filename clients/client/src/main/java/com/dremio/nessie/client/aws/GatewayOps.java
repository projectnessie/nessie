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

package com.dremio.nessie.client.aws;

import com.dremio.nessie.model.Branch;
import com.dremio.nessie.model.NessieConfiguration;
import software.amazon.awssdk.http.SdkHttpMethod;

public enum GatewayOps {

  CONFIG(SdkHttpMethod.GET, "config", true, NessieConfiguration.class),
  CREATE_BRANCH(SdkHttpMethod.POST, "objects", false, String.class),
  DELETE_BRANCH(SdkHttpMethod.DELETE, "objects", false, String.class),
  GET_BRANCH(SdkHttpMethod.GET, "objects", true, Branch.class),
  LIST_BRANCHES(SdkHttpMethod.GET, "objects", true, Branch[].class);

  private final SdkHttpMethod method;
  private final String name;
  private final boolean json;
  private final Class<?> returnType;

  GatewayOps(SdkHttpMethod method, String name, boolean json, Class<?> returnType) {

    this.method = method;
    this.name = name;
    this.json = json;
    this.returnType = returnType;
  }

  public SdkHttpMethod getMethod() {
    return method;
  }

  public String getName() {
    return name;
  }

  public boolean isJson() {
    return json;
  }

  public Class<?> getReturnType() {
    return returnType;
  }
}
