/*
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.reactivesocket.netty.tcp;

import io.reactivesocket.ConnectionSetupHandler;
import io.reactivesocket.ConnectionSetupPayload;
import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.RequestHandler;
import io.reactivesocket.netty.tcp.client.TcpReactiveSocketFactory;
import io.reactivesocket.netty.tcp.server.ReactiveSocketTcpServer;
import org.junit.rules.ExternalResource;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import rx.RxReactiveStreams;

import java.net.SocketAddress;

public class ClientSetupRule extends ExternalResource {

    private TcpReactiveSocketFactory client;
    private ReactiveSocketTcpServer server;
    private SocketAddress serverAddress;
    private ReactiveSocket reactiveSocket;

    @Override
    public Statement apply(final Statement base, Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                server = ReactiveSocketTcpServer.create(0);
                serverAddress = server.start(new ConnectionSetupHandler() {
                    @Override
                    public RequestHandler apply(ConnectionSetupPayload s, ReactiveSocket rs) {
                        return new TestRequestHandler();
                    }
                }).getServerAddress();

                client = TcpReactiveSocketFactory.create(ConnectionSetupPayload.create("", ""));
                reactiveSocket = RxReactiveStreams.toObservable(client.call(serverAddress))
                                                  .toSingle().toBlocking().value();

                base.evaluate();
            }
        };
    }

    public TcpReactiveSocketFactory getClient() {
        return client;
    }

    public ReactiveSocketTcpServer getServer() {
        return server;
    }

    public SocketAddress getServerAddress() {
        return serverAddress;
    }

    public ReactiveSocket getReactiveSocket() {
        return reactiveSocket;
    }
}
