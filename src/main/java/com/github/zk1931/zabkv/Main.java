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

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.omg.PortableInterceptor.SYSTEM_EXCEPTION;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * zabkv starts here.
 */
public final class Main {
    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        // Options for command arguments.
        Options options = new Options();

        Option port = OptionBuilder.withArgName("port")
                .hasArg(true)
                .isRequired(true)
                .withDescription("port number")
                .create("port");

        Option ip = OptionBuilder.withArgName("ip")
                .hasArg(true)
                .isRequired(true)
                .withDescription("current ip address")
                .create("ip");

        Option join = OptionBuilder.withArgName("join")
                .hasArg(true)
                .withDescription("the addr of server to join.")
                .create("join");

        Option help = OptionBuilder.withArgName("h")
                .hasArg(false)
                .withLongOpt("help")
                .withDescription("print out usages.")
                .create("h");

        options.addOption(port)
                .addOption(ip)
                .addOption(join)
                .addOption(help);

        CommandLineParser parser = new BasicParser();
        CommandLine cmd;


        try {
            cmd = parser.parse(options, args);
            if (cmd.hasOption("h")) {
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp("zabkv", options);
                return;
            }
        } catch (ParseException exp) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("zabkv", options);
            return;
        }

        int zabPort = Integer.parseInt(cmd.getOptionValue("port"));
        String myIp = cmd.getOptionValue("ip");

        if ( zabPort < 5000 && zabPort >= 5010 ) {
            System.err.println("port parameter can have value only between 5000 & 5010");
            System.exit(1);
        }

        int serverPort = zabPort%5000 + 8000;

        Database db = new Database(myIp, zabPort,
                cmd.getOptionValue("join"));

        Server server = new Server(serverPort);
        ServletHandler handler = new ServletHandler();
        server.setHandler(handler);
        ServletHolder holder = new ServletHolder(new RequestHandler(db));
        handler.addServletWithMapping(holder, "/*");
        server.start();
        server.join();
        System.out.println("hi");
    }
}
