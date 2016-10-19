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

import java.util.HashMap;

/**
 * Command to put key-value pairs to database.
 */
public final class JsonPutCommand extends Command {
    private static final long serialVersionUID = 0L;

    final String key;
    final String value;

    public JsonPutCommand(String key, String value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public void execute(Database db) {
        HashMap<String, String> map = new HashMap<>();
        map.put(key, value);
        db.put(map);
    }
}
