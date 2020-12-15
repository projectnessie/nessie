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
package com.dremio.nessie.versioned.store.mongodb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.Assertions;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.MongoDatabase;

import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.MongosExecutable;
import de.flapdoodle.embed.mongo.MongosProcess;
import de.flapdoodle.embed.mongo.config.MongoCmdOptions;
import de.flapdoodle.embed.mongo.config.MongodConfig;
import de.flapdoodle.embed.mongo.config.MongosConfig;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.config.Storage;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.process.runtime.Network;

/**
 * Creates and configures a sharded flapdoodle MongoDB instance.
 */
class LocalMongoS extends LocalMongoBase {
  private static final String MONGODB_INFO = "mongodbS-local-info";
  private static final String REPLICA_SET_CONFIG_NAME = "config_replicas";
  private static final String REPLICA_SET_NAME_1 = "replicas_1";
  private static final String REPLICA_SET_NAME_2 = "replicas_2";
  private static final int NUM_REPLICAS = 3;

  private static class MongoSHolder implements ServerHolder {
    private String connectionString;
    private MongosProcess mongosProcess;
    private List<MongodProcess> mongodProcessList;

    MongoSHolder() {
      mongodProcessList = new ArrayList<>(NUM_REPLICAS);
    }

    @Override
    public String getConnectionString() {
      return connectionString;
    }

    @Override
    public void start() throws Exception {
      this.mongodProcessList = Collections.synchronizedList(new ArrayList<>());

      final ExecutorService service = Executors.newFixedThreadPool(NUM_REPLICAS + 1 /*for configReplicaSet*/);
      final ImmutableList.Builder<Future<?>> futureBuilder = ImmutableList.builder();
      final List<Throwable> exceptions = new ArrayList<>();

      final List<MongodConfig> configReplicaSet = createConfigReplicaSet(REPLICA_SET_CONFIG_NAME);

      // Workaround to kick off the download of the mongodb release once and place it in the artifact store.
      MONGOD_STARTER.prepare(configReplicaSet.get(0));

      futureBuilder.add(service.submit(() -> {
        try {
          initializeReplicaSet(REPLICA_SET_CONFIG_NAME, configReplicaSet);
        } catch (Throwable e) {
          exceptions.add(e);
        }
      }));

      final Map<String, List<MongodConfig>> replicaSets = createShardSets(REPLICA_SET_NAME_1, REPLICA_SET_NAME_2);
      for (Map.Entry<String, List<MongodConfig>> entry : replicaSets.entrySet()) {
        futureBuilder.add(service.submit(() -> {
          try {
            initializeReplicaSet(entry.getKey(), entry.getValue());
          } catch (Throwable e) {
            exceptions.add(e);
          }
        }));
      }


      service.shutdown();
      for (Future<?> future : futureBuilder.build()) {
        future.get();
      }

      if (!exceptions.isEmpty()) {
        Assertions.fail(String.format("Errors starting replica set: %s",
            exceptions.stream().map(Throwable::toString).collect(Collectors.joining(", "))));
      }

      initializeMongos(REPLICA_SET_CONFIG_NAME, configReplicaSet);
      configureMongos(replicaSets);

      connectionString = String.format("mongodb://%s:%d",
          mongosProcess.getConfig().net().getServerAddress().getHostAddress(),
          mongosProcess.getConfig().net().getPort());
    }

    @Override
    public void stop() throws ExecutionException, InterruptedException {
      final ExecutorService service = Executors.newFixedThreadPool(mongodProcessList.size());
      final ImmutableList.Builder<Future<?>> futureBuilder = ImmutableList.builder();

      this.mongodProcessList.forEach(p -> futureBuilder.add(service.submit(p::stop)));

      // Wait for everything to stop.
      service.shutdown();
      for (Future<?> future : futureBuilder.build()) {
        future.get();
      }

      this.mongosProcess.stop();
    }

    private void initializeMongos(String name, List<MongodConfig> replicaSets) throws IOException {
      final StringBuilder command = new StringBuilder();
      boolean isFirst = true;

      for (final MongodConfig config : replicaSets) {
        if (isFirst) {
          isFirst = false;
        } else {
          command.append(",");
        }
        command.append(config.net().getServerAddress().getHostName()).append(":").append(config.net().getPort());
      }

      final int port = Network.getFreeServerPort();
      final MongosConfig config = MongosConfig.builder()
          .version(Version.Main.PRODUCTION)
          .replicaSet(name)
          .configDB(command.toString())
          .net(new Net(port, Network.localhostIsIPv6()))
          .build();

      final MongosExecutable mongoExec = MONGOS_STARTER.prepare(config);
      mongosProcess = mongoExec.start();
    }

    private void configureMongos(Map<String, List<MongodConfig>> replicaSets) throws Exception {
      final MongoClient mongo = MongoClients.create(String.format("mongodb://%s:%d",
          mongosProcess.getConfig().net().getServerAddress().getHostAddress(),
          mongosProcess.getConfig().net().getPort()));
      final MongoDatabase mongoAdminDB = mongo.getDatabase("admin");

      // Add shard from the replica set list
      for (Map.Entry<String, List<MongodConfig>> entry : replicaSets.entrySet()) {
        final String replicaName = entry.getKey();
        final StringBuilder builder = new StringBuilder();
        for (MongodConfig mongodConfig : entry.getValue()) {
          if (builder.length() == 0) {
            builder.append(replicaName).append("/");
          } else {
            builder.append(",");
          }
          builder.append(mongodConfig.net().getServerAddress().getHostName());
          builder.append(":");
          builder.append(mongodConfig.net().getPort());
        }

        runCommand(mongoAdminDB, new BasicDBObject("addShard", builder.toString()));
      }

      // Create a store config to grab the defaults of the table names, ignore the connection string.
      final MongoStoreConfig config = MongoStoreConfig.builder().connectionString("").build();

      // Enabled sharding at database level.
      runCommand(mongoAdminDB, new BasicDBObject("enableSharding", config.getDatabaseName()));

      // Create index in sharded collection
      final MongoDatabase db = mongo.getDatabase(config.getDatabaseName());
      final List<String> names = ImmutableList.<String>builder().add(
            config.getKeyListTableName(),
            config.getL1TableName(),
            config.getL2TableName(),
            config.getL3TableName(),
            config.getMetadataTableName(),
            config.getRefTableName(),
            config.getValueTableName())
          .build();
      for (String name : names) {
        db.getCollection(name).createIndex(new BasicDBObject("id", 1));

        // Shard the collection
        final BasicDBObject cmd = new BasicDBObject();
        cmd.put("shardCollection", config.getDatabaseName() + "." + name);
        cmd.put("key", new BasicDBObject("id", 1));
        runCommand(mongoAdminDB, cmd);
      }
    }

    private List<Document> runCommand(MongoDatabase mongoAdminDB, Bson command) throws Exception {
      final MongoDBStore.AwaitableListSubscriber<Document> subscriber = new MongoDBStore.AwaitableListSubscriber<>();
      mongoAdminDB.runCommand(command).subscribe(subscriber);
      subscriber.await(5000);
      return subscriber.getReceived();
    }

    private List<MongodConfig> createConfigReplicaSet(String name) throws IOException {
      final ImmutableList.Builder<MongodConfig> builder = ImmutableList.builder();
      for (int i = 0; i < NUM_REPLICAS; ++i) {
        builder.add(createConfig(true, name));
      }
      return builder.build();
    }

    private Map<String, List<MongodConfig>> createShardSets(String... names) throws IOException {
      final ImmutableMap.Builder<String, List<MongodConfig>> builder = ImmutableMap.builder();
      for (String name : names) {
        final ImmutableList.Builder<MongodConfig> listBuilder = ImmutableList.builder();
        for (int i = 0; i < NUM_REPLICAS; ++i) {
          listBuilder.add(createConfig(false, name));
        }

        builder.put(name, listBuilder.build());
      }

      return builder.build();
    }

    private MongodConfig createConfig(boolean isConfigServer, String replicaName) throws IOException {
      final MongoCmdOptions cmdOptions = MongoCmdOptions.builder()
          .useSmallFiles(!isConfigServer)
          .useNoJournal(false)
          .build();

      // 1MB oplogsize should be enough.
      final Storage replication = new Storage(null, replicaName, 1);

      return MongodConfig.builder()
          .version(Version.Main.PRODUCTION)
          .net(new Net(Network.getFreeServerPort(), Network.localhostIsIPv6()))
          .isConfigServer(isConfigServer)
          .isShardServer(!isConfigServer)
          .replication(replication)
          .cmdOptions(cmdOptions)
          .build();
    }

    private void initializeReplicaSet(String replicaName, List<MongodConfig> mongoConfigList) throws Exception {
      if (mongoConfigList.size() < 3) {
        throw new Exception("A replica set must contain at least 3 members.");
      }

      final ExecutorService service = Executors.newFixedThreadPool(mongoConfigList.size());
      final ImmutableList.Builder<Future<?>> futureBuilder = ImmutableList.builder();
      final List<Throwable> exceptions = new ArrayList<>();

      // Create 3 mongod processes
      for (MongodConfig mongoConfig : mongoConfigList) {
        if (!mongoConfig.replication().getReplSetName().equals(replicaName)) {
          throw new Exception("Replica set name must match in mongo configuration");
        }

        futureBuilder.add(service.submit(() -> {
          try {
            final MongodExecutable mongodExe = MONGOD_STARTER.prepare(mongoConfig);
            final MongodProcess process = mongodExe.start();
            mongodProcessList.add(process);
          } catch (Exception e) {
            exceptions.add(e);
          }
        }));
      }

      service.shutdown();
      for (Future<?> future : futureBuilder.build()) {
        future.get();
      }

      if (!exceptions.isEmpty()) {
        Assertions.fail();
      }

      Thread.sleep(1000);

      final MongoClient mongo = MongoClients.create(String.format("mongodb://%s:%d",
          mongoConfigList.get(0).net().getServerAddress().getHostName(), mongoConfigList.get(0).net().getPort()));
      final MongoDatabase mongoAdminDB = mongo.getDatabase("admin");

      // Build BSON object replica set settings
      final DBObject replicaSetSetting = new BasicDBObject();
      replicaSetSetting.put("_id", replicaName);
      BasicDBList members = new BasicDBList();

      int i = 0;
      for (MongodConfig mongoConfig : mongoConfigList) {
        final DBObject host = new BasicDBObject();
        host.put("_id", i++);
        host.put("host", mongoConfig.net().getServerAddress().getHostName() + ":" + mongoConfig.net().getPort());
        members.add(host);
      }

      replicaSetSetting.put("members", members);
      runCommand(mongoAdminDB, new BasicDBObject("replSetInitiate", replicaSetSetting));

      // Check replica set status before to proceed
      while (!isReplicaSetStarted(mongoAdminDB)) {
        Thread.sleep(1000);
      }

      mongo.close();
    }

    private boolean isReplicaSetStarted(MongoDatabase mongoAdminDB) throws Exception {
      final Document setting = runCommand(mongoAdminDB, new BasicDBObject("replSetGetStatus", 1)).get(0);

      final List<Document> members = (List<Document>)setting.get("members");
      if (members == null) {
        return false;
      }

      boolean isPrimarySet = false;
      for (Document member : members) {
        final int state = member.getInteger("state");
        // 1 - PRIMARY, 2 - SECONDARY, 7 - ARBITER
        if (state == 1) {
          isPrimarySet = true;
        }
        if (state != 1 && state != 2 && state != 7) {
          return false;
        }
      }

      return isPrimarySet;
    }
  }

  @Override
  protected String getIdentifier() {
    return MONGODB_INFO;
  }

  @Override
  protected ServerHolder createHolder() {
    return new MongoSHolder();
  }
}
