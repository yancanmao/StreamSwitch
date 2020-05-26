/*
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

package Nexmark.queries;

import Nexmark.sinks.DummyLatencyCountingSink;
import Nexmark.sources.keyed.KeyedBidSourceFunction;
import org.apache.beam.sdk.nexmark.model.Bid;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Query2Keyed {

    private static final Logger logger  = LoggerFactory.getLogger(Query2Keyed.class);

    public static void main(String[] args) throws Exception {

        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.disableOperatorChaining();

        // enable latency tracking
        env.getConfig().setLatencyTrackingInterval(5000);

        // sin curve configs
        // srcRate is amplitude of sin.
        final int srcRate = params.getInt("srcRate", 100000);
        // srcCycle is the period of sin.
        final int srcCycle = params.getInt("srcCycle", 60);
        // srcBase is the minimum arrival rate of sin.
        final int srcBase = params.getInt("srcBase", 0);
        // warmup time is to make app warm up with a constant rate in a time interval.
        final int srcWarmUp = params.getInt("srcWarmUp", 100);
        // tuple size is to config the size of each tuple.
        final int srcTupleSize = params.getInt("srcTupleSize", 100);

        DataStream<Tuple2<Long, Bid>> bids = env.addSource(new KeyedBidSourceFunction(srcRate, srcCycle, srcBase, srcWarmUp*1000, srcTupleSize))
                .setParallelism(params.getInt("p-source", 1))
                .setMaxParallelism(params.getInt("mp", 64))
                .keyBy(0);


        // SELECT Rstream(auction, price)
        // FROM Bid [NOW]
        // WHERE auction = 1007 OR auction = 1020 OR auction = 2001 OR auction = 2019 OR auction = 2087;

        DataStream<Tuple2<Long, Long>> converted = bids
                .flatMap(new FlatMapFunction<Tuple2<Long, Bid>, Tuple2<Long, Long>>() {
                    @Override
                    public void flatMap(Tuple2<Long, Bid> bid, Collector<Tuple2<Long, Long>> out) throws Exception {
                        if(bid.f1.auction % 1007 == 0 || bid.f1.auction % 1020 == 0 || bid.f1.auction % 2001 == 0 || bid.f1.auction % 2019 == 0 || bid.f1.auction % 2087 == 0) {
                            out.collect(new Tuple2<>(bid.f1.auction, bid.f1.price));
                        }
                    }
                }).setMaxParallelism(params.getInt("mp", 64))
                .setParallelism(params.getInt("p2", 1))
                .name("flatmap")
                .uid("flatmap");

        GenericTypeInfo<Object> objectTypeInfo = new GenericTypeInfo<>(Object.class);
        converted.transform("DummyLatencySink", objectTypeInfo, new DummyLatencyCountingSink<>(logger))
                .setParallelism(params.getInt("p-flatMap", 1));

        // execute program
        env.execute("Nexmark Query2");
    }

}