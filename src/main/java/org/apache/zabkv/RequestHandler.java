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

  // RequestHandler doesn't get initialized until the first request is made. We
  // probably want to initialize the Database object before the first request
  // comes in. Otherwise the first request takes a long time due to recovery.
  private Database db = new Database();

  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    // remove the leading slash from the request path and use that as the key.
    String key = request.getPathInfo().substring(1);
    byte[] value = db.get(key);
    response.setContentType("text/html");
    if (value == null) {
      response.setStatus(HttpServletResponse.SC_NOT_FOUND);
    } else {
      response.setStatus(HttpServletResponse.SC_OK);
      response.setContentLength(value.length);
      response.getOutputStream().write(value);
    }
  }

  @Override
  protected void doPut(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    AsyncContext context = request.startAsync(request, response);
    // remove the leading slash from the request path and use that as the key.
    String key = request.getPathInfo().substring(1);
    int length = request.getContentLength();
    if (length < 0) {
      // Don't accept requests without content length.
      response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      response.setContentLength(0);
      return;
    }
    byte[] value = new byte[length];
    request.getInputStream().read(value);
    PutCommand command = new PutCommand(key, value);
    db.add(command, context);
  }
}
