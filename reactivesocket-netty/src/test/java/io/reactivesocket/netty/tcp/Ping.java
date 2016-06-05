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

import io.netty.buffer.ByteBuf;
import io.reactivesocket.ConnectionSetupPayload;
import io.reactivesocket.Payload;
import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.netty.tcp.client.TcpReactiveSocketFactory;
import io.reactivex.netty.protocol.tcp.client.TcpClient;
import org.HdrHistogram.Recorder;
import rx.Observable;
import rx.observers.TestSubscriber;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static rx.RxReactiveStreams.*;

public class Ping {
    public static void main(String... args) throws Exception {

        Payload keyPayload = new PayloadImpl("hello");
        TcpReactiveSocketFactory factory = TcpReactiveSocketFactory.create(
                new Function<SocketAddress, TcpClient<ByteBuf, ByteBuf>>() {
                    @Override
                    public TcpClient<ByteBuf, ByteBuf> apply(SocketAddress socketAddress) {
                        return TcpClient.newClient(socketAddress);
                    }
                }, ConnectionSetupPayload.create("", ""), Throwable::printStackTrace);

        ReactiveSocket reactiveSocket =
                toObservable(factory.call(new InetSocketAddress("localhost", 7878)))
                                 .toSingle()
                                 .toBlocking().value();

        int n = 1_000_000;
        final Recorder histogram = new Recorder(3600000000000L, 3);

        Observable.interval(1, 1, TimeUnit.SECONDS)
                  .forEach(aLong -> {
                      System.out.println("---- PING/ PONG HISTO ----");
                      histogram.getIntervalHistogram()
                               .outputPercentileDistribution(System.out, 5, 1000.0, false);
                      System.out.println("---- PING/ PONG HISTO ----");
                  });

        TestSubscriber<Payload> sub = new TestSubscriber<>();
        Observable.range(1, Integer.MAX_VALUE)
                  .flatMap(i -> {
                      long start = System.nanoTime();
                      keyPayload.getData().rewind();
                      return toObservable(reactiveSocket.requestResponse(keyPayload))
                              .doOnError(t -> t.printStackTrace())
                              .doOnNext(s -> {
                                  long diff = System.nanoTime() - start;
                                  histogram.recordValue(diff);
                              });
                  }, 16)
                  .doOnError(t -> t.printStackTrace())
                  .take(n)
                  .subscribe(sub);

        sub.awaitTerminalEvent(1, TimeUnit.HOURS);
    }
}
