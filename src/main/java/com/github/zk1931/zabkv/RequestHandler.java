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

import java.io.IOException;
import javax.servlet.AsyncContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Request handler.
 */
public final class RequestHandler extends HttpServlet {
    private static final Logger LOG =
            LoggerFactory.getLogger(RequestHandler.class);

    private final Database db;

    RequestHandler(Database db) {
        this.db = db;
    }

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        // remove the leading slash from the request path and use that as the key.
        String key = request.getHeader("key");
        LOG.info("Got GET request for key {}", key);
        String value = null;
        if (key == null || key.equals("")) {
            // Gets all the key-value pairs.
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        } else {
            value = db.get(key);
        }
        response.setContentType("text/html");
        if (value == null) {
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
        } else {
            response.setStatus(HttpServletResponse.SC_OK);
            response.setContentLength(value.length());
            response.getOutputStream().write(value.getBytes());
        }
    }

    @Override
    protected void doPut(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        AsyncContext context = request.startAsync(request, response);
        // remove the leading slash from the request path and use that as the key.
        int length = request.getContentLength();
        String key = request.getHeader("key");
        if (length < 0 || key == null || key == "" ) {
            // Don't accept requests without content length.
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            response.setContentLength(0);
            context.complete();
            return;
        }
        byte[] value = new byte[length];
        request.getInputStream().read(value);
        String valueString = new String(value);
        LOG.info("Got PUT request : {} => {}", key, valueString);
        JsonPutCommand command = new JsonPutCommand(key, valueString);
        if(!db.add(command, context)) {
            response.setContentType("text/html");
            response.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
            context.complete();
        }
    }

    @Override
    protected void doDelete(HttpServletRequest request,
                            HttpServletResponse response)
            throws ServletException, IOException {
        AsyncContext context = request.startAsync(request, response);
        String key = request.getHeader("key");
        if (key == null || key == "" ) {
            // Don't accept requests without content length.
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            response.setContentLength(0);
            context.complete();
            return;
        }

        LOG.info("Got DELETE request for key {}", key);
        DeleteCommand command = new DeleteCommand(key);
        if(!db.add(command, context)) {
            response.setContentType("text/html");
            response.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
            context.complete();
        }
    }
}
