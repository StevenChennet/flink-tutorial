package com.stevenchennet.net.flink191.redising;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisStringAsyncCommands;
import io.lettuce.core.api.reactive.RedisStringReactiveCommands;
import io.lettuce.core.api.sync.RedisStringCommands;
import reactor.core.publisher.Mono;

public class BasicLettuceApp {
    public static void main(String[] args) {
        RedisClient client = createRedisClient();

        basicUsage(client);

        client.shutdown();
    }

    static void basicUsage(RedisClient client) {
        // https://github.com/lettuce-io/lettuce-core/wiki/Basic-usage

        StatefulRedisConnection<String, String> connection = client.connect();

        RedisStringCommands sync = connection.sync();
        String syncResult = sync.set("key","value");
        Object value = sync.get("key");

        connection.close();
    }

    static void asynUsage(RedisClient client){
        StatefulRedisConnection<String, String> connection = client.connect();
        RedisStringAsyncCommands<String, String> async = connection.async();
        RedisFuture<String> set = async.set("key", "value");
        RedisFuture<String> get = async.get("key");

        //async.awaitAll(set, get) == true
        //set.get() == "OK"
        //get.get() == "value"


    }

    static void reactiveUsage(RedisClient client){
        StatefulRedisConnection<String, String> connection = client.connect();
        RedisStringReactiveCommands<String, String> reactive = connection.reactive();
        Mono<String> set = reactive.set("key", "value");
        Mono<String> get = reactive.get("key");

        set.subscribe();

        //get.block() == "value"
    }

    static RedisClient createRedisClient(){
        //RedisURI redisURI = RedisURI.builder().withHost("").withPort(6379).withPassword("").withDatabase(15).build();
        RedisURI redisURI = RedisURI.builder().withHost("").withPort(6379).withPassword("").withDatabase(15).build();
        RedisClient client = RedisClient.create(redisURI);
        return client;
    }
}
