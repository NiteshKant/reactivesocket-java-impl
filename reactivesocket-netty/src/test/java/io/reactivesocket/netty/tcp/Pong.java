/**
 * Copyright 2015 Netflix, Inc.
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
package io.reactivesocket.netty.tcp;

import io.reactivesocket.ConnectionSetupHandler;
import io.reactivesocket.ConnectionSetupPayload;
import io.reactivesocket.Payload;
import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.RequestHandler;
import io.reactivesocket.netty.tcp.server.ReactiveSocketTcpServer;
import io.reactivex.netty.protocol.tcp.server.TcpServer;
import org.reactivestreams.Publisher;
import rx.Observable;

import java.util.Random;

import static rx.RxReactiveStreams.*;

public final class Pong {

    public static void main(String... args) throws Exception {
        byte[] response = new byte[1024];
        Random r = new Random();
        r.nextBytes(response);

        ReactiveSocketTcpServer.create(TcpServer.newServer(7878))
                               .start(new ConnectionSetupHandler() {
                                   @Override
                                   public RequestHandler apply(ConnectionSetupPayload setupPayload,
                                                               ReactiveSocket reactiveSocket) {
                                       return new TestRequestHandler() {
                                           @Override
                                           public Publisher<Payload> handleRequestResponse(Payload payload) {
                                               return toPublisher(Observable.just(new PayloadImpl(response)));
                                           }
                                       };
                                   }
                               })
                               .awaitShutdown();
    }

}
