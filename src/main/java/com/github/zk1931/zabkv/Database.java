/**
 * Licensed to the zk9131 under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.zk1931.zabkv;


import com.github.zk1931.jzab.*;
import com.github.zk1931.jzab.PendingRequests.Tuple;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.bindings.StringBinding;
import jetbrains.exodus.env.*;
import jetbrains.exodus.env.Transaction;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.AsyncContext;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * StateMachine of the ZabKV.
 */
public final class Database {
    private static final Logger LOG = LoggerFactory.getLogger(Database.class);
    private final DatabaseStateMachine stateMachine;
    private final Environment storeEnv;

    private Zab zab;

    private String serverId;

    private final ZabConfig config = new ZabConfig();

    private ConcurrentSkipListMap<String, byte[]> kvstore =
            new ConcurrentSkipListMap<>();

    public Database(String serverId, String joinPeer, String logDir) {
        try {
            this.serverId = serverId;
            String zabCoord = "localhost:" + this.serverId;

            if (this.serverId != null && joinPeer == null) {
                // It's the first server in cluster, joins itself.
                joinPeer = zabCoord;
            }

            config.setLogDir(logDir);
            this.stateMachine = new DatabaseStateMachine(this);

            if (joinPeer != null) {
                zab = new Zab(stateMachine, config, zabCoord, joinPeer);
            } else {
                // Recovers from log directory.
                zab = new Zab(stateMachine, config);
            }
            this.serverId = zab.getServerId();

            storeEnv = Environments.newInstance("/tmp/my-db/" + serverId + "/data");

        } catch (Exception ex) {
            LOG.error("Caught exception : ", ex);
            throw new RuntimeException();
        }
    }

    public String get(String key) {
        Transaction txn = storeEnv.beginExclusiveTransaction();
        final Store store = storeEnv.openStore("KV", StoreConfig.WITHOUT_DUPLICATES, txn);
        ByteIterable val = store.get(txn, StringBinding.stringToEntry(key));

        try {
            return StringBinding.entryToString(val);
        } finally {
            txn.flush();
        }
    }

    public void put(final Map<String, String> updates) {
        storeEnv.executeInTransaction(new TransactionalExecutable() {
            @Override
            public void execute(@NotNull final Transaction txn) {
                final Store store = storeEnv.openStore("KV", StoreConfig.WITHOUT_DUPLICATES, txn);
                for ( Map.Entry<String, String> entry : updates.entrySet() ) {
                    store.put(txn, StringBinding.stringToEntry(entry.getKey()), StringBinding.stringToEntry(entry.getValue()));
                }
            }
        });

//        kvstore.putAll(updates);
    }

//    public String getAll() throws IOException {
//        GsonBuilder builder = new GsonBuilder();
//        Gson gson = builder.create();
//        return gson.toJson(kvstore);
//    }

    public void delete(String key) {
        this.kvstore.remove(key);
    }

    public boolean add(Command command, AsyncContext context) {
        try {
            ByteBuffer bb = Serializer.serialize(command);
            try {
                zab.send(bb, context);
            } catch (ZabException ex) {
                return false;
            }
        } catch (IOException ex) {
            return false;
        }
        return true;
    }


    public final static class DatabaseStateMachine implements StateMachine {

        private final Database db;

        public DatabaseStateMachine(Database database) {
            this.db = database;
        }

        @Override
        public ByteBuffer preprocess(Zxid zxid, ByteBuffer message) {
            LOG.debug("Preprocessing a message: {}", message);
            return message;
        }

        @Override
        public void deliver(Zxid zxid, ByteBuffer stateUpdate, String clientId,
                            Object ctx) {
            Command command = Serializer.deserialize(stateUpdate);
            command.execute(db);
            AsyncContext context = (AsyncContext) ctx;
            LOG.info("Deliver executed" );
            if (context == null) {
                // This request is sent from other instance.
                return;
            }
            HttpServletResponse response =
                    (HttpServletResponse) (context.getResponse());
            response.setContentType("text/html");
            response.setStatus(HttpServletResponse.SC_OK);
            context.complete();
        }

        @Override
        public void flushed(ByteBuffer request, Object ctx) {
        }

        @Override
        public void save(OutputStream os) {
            // No support for snapshot yet.
        }

        @Override
        public void restore(InputStream is) {
            // No support for snapshot yet.
        }

        @Override
        public void snapshotDone(String filePath, Object ctx) {
        }

        @Override
        public void removed(String peerId, Object ctx) {
        }

        @Override
        public void recovering(PendingRequests pendingRequests) {
            LOG.info("Recovering...");
            // Returns error for all pending requests.
            for (Tuple tp : pendingRequests.pendingSends) {
                AsyncContext context = (AsyncContext) tp.param;
                HttpServletResponse response =
                        (HttpServletResponse) (context.getResponse());
                response.setContentType("text/html");
                response.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
                context.complete();
            }
        }

        @Override
        public void leading(Set<String> activeFollowers, Set<String> clusterMembers) {
            LOG.info("LEADING with active followers : ");
            for (String peer : activeFollowers) {
                LOG.info(" -- {}", peer);
            }
            LOG.info("Cluster configuration change : ", clusterMembers.size());
            for (String peer : clusterMembers) {
                LOG.info(" -- {}", peer);
            }
        }

        @Override
        public void following(String leader, Set<String> clusterMembers) {
            LOG.info("FOLLOWING {}", leader);
            LOG.info("Cluster configuration change : ", clusterMembers.size());
            for (String peer : clusterMembers) {
                LOG.info(" -- {}", peer);
            }
        }
    }
}
