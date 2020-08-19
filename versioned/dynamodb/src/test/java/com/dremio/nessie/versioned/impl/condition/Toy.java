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
