/**
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
 */
package io.reactivesocket.netty.tcp.client;

import io.netty.buffer.ByteBuf;
import io.reactivesocket.ConnectionSetupPayload;
import io.reactivesocket.Frame;
import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.ReactiveSocketFactory;
import io.reactivesocket.netty.ReactiveSocketFrameCodec;
import io.reactivesocket.netty.tcp.ReactiveSocketLengthCodec;
import io.reactivesocket.netty.tcp.TcpDuplexConnection;
import io.reactivesocket.rx.Completable;
import io.reactivex.netty.channel.Connection;
import io.reactivex.netty.protocol.tcp.client.TcpClient;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.RxReactiveStreams;
import rx.Single;
import rx.Single.OnSubscribe;
import rx.SingleSubscriber;
import rx.Subscriber;

import java.net.SocketAddress;
import java.util.function.Consumer;
import java.util.function.Function;

import static io.reactivesocket.DefaultReactiveSocket.*;

public class TcpReactiveSocketFactory implements ReactiveSocketFactory<SocketAddress, ReactiveSocket> {

    private static final Logger logger = LoggerFactory.getLogger(TcpReactiveSocketFactory.class);

    private final Function<SocketAddress, TcpClient<ByteBuf, ByteBuf>> clientFactory;
    private final ConnectionSetupPayload setup;
    private final Consumer<Throwable> errorStream;

    public TcpReactiveSocketFactory(Function<SocketAddress, TcpClient<ByteBuf, ByteBuf>> clientFactory,
                                    ConnectionSetupPayload connectionSetupPayload, Consumer<Throwable> errorStream) {
        this.clientFactory = clientFactory;
        setup = connectionSetupPayload;
        this.errorStream = errorStream;
    }

    @Override
    public Publisher<ReactiveSocket> call(SocketAddress address) {
        TcpClient<Frame, Frame> client = clientFactory.apply(address)
                                                      .addChannelHandlerLast("length-codec",
                                                                             ReactiveSocketLengthCodec::new)
                                                      .addChannelHandlerLast("frame-codec",
                                                                             ReactiveSocketFrameCodec::new);

        Single<ReactiveSocket> r = Single.create(new OnSubscribe<ReactiveSocket>() {
            @Override
            public void call(SingleSubscriber<? super ReactiveSocket> s) {
                client.createConnectionRequest()
                      .toSingle()
                      .unsafeSubscribe(new Subscriber<Connection<Frame, Frame>>() {
                          @Override
                          public void onCompleted() {
                              // Single contract does not allow complete without onNext and onNext here completes
                              // the outer subscriber
                          }

                          @Override
                          public void onError(Throwable e) {
                              s.onError(e);
                          }

                          @Override
                          public void onNext(Connection<Frame, Frame> c) {
                              TcpDuplexConnection dc = new TcpDuplexConnection(c);
                              ReactiveSocket rs = fromClientConnection(dc, setup, errorStream);
                              rs.start(new Completable() {
                                  @Override
                                  public void success() {
                                      s.onSuccess(rs);
                                  }

                                  @Override
                                  public void error(Throwable e) {
                                      s.onError(e);
                                  }
                              });
                          }
                      });
            }
        });
        return RxReactiveStreams.toPublisher(r.toObservable());
    }

    public static TcpReactiveSocketFactory create(ConnectionSetupPayload setupPayload) {
        return create(socketAddress -> {
            return TcpClient.newClient(socketAddress);
        }, setupPayload, new Consumer<Throwable>() {
            @Override
            public void accept(Throwable throwable) {
                logger.error("Error received on ReactiveSocket.", throwable);
            }
        });
    }

    public static TcpReactiveSocketFactory create(ConnectionSetupPayload setupPayload, Consumer<Throwable> errorStream) {
        return create(socketAddress -> {
            return TcpClient.newClient(socketAddress);
        }, setupPayload, errorStream);
    }

    public static TcpReactiveSocketFactory create(Function<SocketAddress, TcpClient<ByteBuf, ByteBuf>> clientFactory,
                                                  ConnectionSetupPayload setupPayload,
                                                  Consumer<Throwable> errorStream) {
        return new TcpReactiveSocketFactory(clientFactory, setupPayload, errorStream);
    }
}
