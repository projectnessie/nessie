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
package com.dremio.nessie.versioned.impl.condition;

import java.util.List;

import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey;

/**
 * A toy implementation of a class to be used for dynamo testing.
 */
@DynamoDbBean
public class Toy {

  private String id;
  private String str;
  private String abort1;
  private byte[] data;
  private List<Bauble> baubles;

  public Toy() {
  }

  @DynamoDbPartitionKey
  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getStr() {
    return str;
  }

  public String getAbort1() {
    return abort1;
  }

  public void setAbort1(String abort1) {
    this.abort1 = abort1;
  }

  public void setStr(String str) {
    this.str = str;
  }

  public byte[] getData() {
    return data;
  }

  public void setData(byte[] data) {
    this.data = data;
  }

  public List<Bauble> getBaubles() {
    return baubles;
  }

  public void setBaubles(List<Bauble> bobbles) {
    this.baubles = bobbles;
  }

  @DynamoDbBean
  public static class Bauble {

    private String name;
    private String id;
    private Integer num;

    public Bauble() {
    }

    public void setName(String name) {
      this.name = name;
    }

    public void setId(String id) {
      this.id = id;
    }

    public void setNum(Integer num) {
      this.num = num;
    }

    public String getName() {
      return name;
    }

    public String getId() {
      return id;
    }

    public Integer getNum() {
      return num;
    }

  }
}
