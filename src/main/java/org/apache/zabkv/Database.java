/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
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

package org.apache.zabkv;

import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Properties;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.LinkedBlockingQueue;
import javax.servlet.AsyncContext;
import javax.servlet.http.HttpServletResponse;
import org.apache.zab.QuorumZab;
import org.apache.zab.StateMachine;
import org.apache.zab.Zab;
import org.apache.zab.Zxid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * state machine.
 */
public final class Database implements StateMachine {
  private static final Logger LOG = LoggerFactory.getLogger(Database.class);

  private Zab zab;

  private ConcurrentSkipListMap<String, byte[]> kvstore =
    new ConcurrentSkipListMap<>();

  private LinkedBlockingQueue<AsyncContext> pending =
    new LinkedBlockingQueue<>();

  public Database() {
    try {

      String serverId = System.getProperty("serverId");
      String servers = System.getProperty("servers");
      String logDir = System.getProperty("logdir");

      if (serverId == null || servers == null) {
        LOG.error("ServerId and servers properties can't be null.");
        throw new RuntimeException("serverId and server can't be null.");
      }

      LOG.debug("Consctructs QuorumZab with serverId : {}, servers : {}, "
          + "logdir : {}", serverId, servers, logDir);

      Properties prop = new Properties();
      prop.setProperty("serverId", serverId);
      prop.setProperty("servers", servers);
      if (logDir != null) {
        prop.setProperty("logdir", logDir);
      }
      zab = new QuorumZab(this, prop);
    } catch (Exception ex) {
      throw new RuntimeException();
    }
  }

  public byte[] get(String key) {
    return (byte[])kvstore.get((Object)key);
  }

  public byte[] put(String key, byte[] value) {
    return kvstore.put(key, value);
  }

  /**
   * Add a request to this database.
   *
   * This method must be synchronized to ensure that the requests are sent to
   * Zab in the same order they get enqueued to the pending queue.
   */
  public synchronized boolean add(PutCommand command, AsyncContext context) {
    if (!pending.add(context)) {
      return false;
    }
    try {
      ByteBuffer bb = command.toByteBuffer();
      LOG.debug("Sending a message: {}", bb);
      zab.send(command.toByteBuffer());
    } catch (IOException ex) {
      throw new RuntimeException();
    }
    return true;
  }

  public ByteBuffer preprocess(Zxid zxid, ByteBuffer message) {
    LOG.debug("Preprocessing a message: {}", message);
    return message;
  }

  public void deliver(Zxid zxid, ByteBuffer stateUpdate) {
    LOG.debug("Received a message: {}", stateUpdate);
    PutCommand command = PutCommand.fromByteBuffer(stateUpdate);
    LOG.debug("Delivering a command: {} {}", zxid, command);
    command.execute(this);
    AsyncContext context = pending.poll();
    if (context == null) {
      // There is no pending HTTP request to respond to.
      return;
    }
    HttpServletResponse response =
      (HttpServletResponse)(context.getResponse());
    response.setContentType("text/html");
    response.setStatus(HttpServletResponse.SC_OK);
    context.complete();
  }

  public void getState(OutputStream os) {
    throw new UnsupportedOperationException();
  }

  public void setState(InputStream is) {
    throw new UnsupportedOperationException();
  }
}