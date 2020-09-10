package com.dremio.nessie.hms;

import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.RuntimeStatsCleanerTask;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.events.EventCleanerTask;
import org.junit.jupiter.api.Test;

public class TestHMS {

  @Test
  public void foo() throws Throwable {
    System.setProperty(ConfVars.RAW_STORE_IMPL.getVarname(), TraceRawStore.class.getCanonicalName());
    System.setProperty(ConfVars.TASK_THREADS_ALWAYS.getVarname(), EventCleanerTask.class.getName() + "," + RuntimeStatsCleanerTask.class.getName());
    System.setProperty(ConfVars.WAREHOUSE.getVarname(), "/tmp/hive/warehouse");
    System.setProperty(ConfVars.WAREHOUSE_EXTERNAL.getVarname(), "/tmp/hive/ewarehouse");
    System.setProperty(ConfVars.EVENT_DB_NOTIFICATION_API_AUTH.getVarname(), "false");
    HiveMetaStore.main(new String[]{});
  }

}
