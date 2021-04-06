package com.github.mihaibogdaneugen.redisrepository;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.JedisPool;

import javax.swing.*;
import java.util.Collections;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class RedisRepositoryConfigurationTests extends RedisTestContainer {

    static JedisPool jedisPool;

    @BeforeAll
    static void beforeAll() {
        jedisPool = new JedisPool(
                REDIS_CONTAINER.getContainerIpAddress(),
                REDIS_CONTAINER.getMappedPort(REDIS_PORT));
    }

    @AfterAll
    static void afterAll() {
        jedisPool.close();
    }

    @Test
    void testNullJedisPool() {
        final var nullError = assertThrows(IllegalArgumentException.class, () ->
                RedisRepositoryConfiguration.builder()
                        .collectionKey(randomString())
                        .build());
        assertEquals("jedisPool cannot be null!", nullError.getMessage());
    }

    @Test
    void testInvalidCollectionKey() {
        final var nullCollectionKeyError = assertThrows(IllegalArgumentException.class, () ->
                RedisRepositoryConfiguration.builder()
                        .jedisPool(jedisPool)
                        .build());
        assertEquals("collectionKey cannot be null, nor empty!", nullCollectionKeyError.getMessage());

        final var emptyCollectionKeyError = assertThrows(IllegalArgumentException.class, () ->
                RedisRepositoryConfiguration.builder()
                        .jedisPool(jedisPool)
                        .collectionKey("")
                        .build());
        assertEquals("collectionKey cannot be null, nor empty!", emptyCollectionKeyError.getMessage());

        final var invalidCollectionKey = randomString() + ":" + randomString();
        final var invalidCollectionKeyError = assertThrows(IllegalArgumentException.class, () ->
                RedisRepositoryConfiguration.builder()
                        .jedisPool(jedisPool)
                        .collectionKey(invalidCollectionKey)
                        .build());
        assertEquals("Collection key `" + invalidCollectionKey + "` cannot contain `:`", invalidCollectionKeyError.getMessage());
    }

    @Test
    void testInvalidStrategy() {
        final var noneStrategyError = assertThrows(IllegalArgumentException.class, () ->
                RedisRepositoryConfiguration.<Person>builder()
                        .jedisPool(jedisPool)
                        .collectionKey("people")
                        .build());
        assertEquals("invalid RedisRepositoryStrategy", noneStrategyError.getMessage());
    }

    @Test
    void testInvalidSerializer() {
        final var nullSerializerErrorStrategyEachEntityIsAValue = assertThrows(IllegalArgumentException.class, () ->
                RedisRepositoryConfiguration.<Person>builder()
                        .jedisPool(jedisPool)
                        .collectionKey("people")
                        .strategyEachEntityIsAValue(RedisRepositoryStrategy.EachEntityIsAValue.<Person>builder()
                                .deserializer(text -> Person.random())
                                .build())
                        .build());
        assertEquals("serializer cannot be null!", nullSerializerErrorStrategyEachEntityIsAValue.getMessage());

        final var nullSerializerErrorStrategyEachEntityIsAHash = assertThrows(IllegalArgumentException.class, () ->
                RedisRepositoryConfiguration.<Person>builder()
                        .jedisPool(jedisPool)
                        .collectionKey("people")
                        .strategyEachEntityIsAHash(RedisRepositoryStrategy.EachEntityIsAHash.<Person>builder()
                                .deserializer(map -> Person.random())
                                .build())
                        .build());
        assertEquals("serializer cannot be null!", nullSerializerErrorStrategyEachEntityIsAHash.getMessage());

        final var nullSerializerErrorStrategyEachEntityIsAValueInAHash = assertThrows(IllegalArgumentException.class, () ->
                RedisRepositoryConfiguration.<Person>builder()
                        .jedisPool(jedisPool)
                        .collectionKey("people")
                        .strategyEachEntityIsAValueInAHash(RedisRepositoryStrategy.EachEntityIsAValueInAHash.<Person>builder()
                                .deserializer(text -> Person.random())
                                .build())
                        .build());
        assertEquals("serializer cannot be null!", nullSerializerErrorStrategyEachEntityIsAValueInAHash.getMessage());

        final var nullSerializerErrorStrategyBinaryEachEntityIsAValue = assertThrows(IllegalArgumentException.class, () ->
                RedisRepositoryConfiguration.<Person>builder()
                        .jedisPool(jedisPool)
                        .collectionKey("people")
                        .strategyBinaryEachEntityIsAValue(RedisRepositoryStrategy.BinaryEachEntityIsAValue.<Person>builder()
                                .deserializer(text -> Person.random())
                                .build())
                        .build());
        assertEquals("serializer cannot be null!", nullSerializerErrorStrategyBinaryEachEntityIsAValue.getMessage());

        final var nullSerializerErrorStrategyBinaryEachEntityIsAHash = assertThrows(IllegalArgumentException.class, () ->
                RedisRepositoryConfiguration.<Person>builder()
                        .jedisPool(jedisPool)
                        .collectionKey("people")
                        .strategyBinaryEachEntityIsAHash(RedisRepositoryStrategy.BinaryEachEntityIsAHash.<Person>builder()
                                .deserializer(map -> Person.random())
                                .build())
                        .build());
        assertEquals("serializer cannot be null!", nullSerializerErrorStrategyBinaryEachEntityIsAHash.getMessage());

        final var nullSerializerErrorStrategyBinaryEachEntityIsAValueInAHash = assertThrows(IllegalArgumentException.class, () ->
                RedisRepositoryConfiguration.<Person>builder()
                        .jedisPool(jedisPool)
                        .collectionKey("people")
                        .strategyBinaryEachEntityIsAValueInAHash(RedisRepositoryStrategy.BinaryEachEntityIsAValueInAHash.<Person>builder()
                                .deserializer(text -> Person.random())
                                .build())
                        .build());
        assertEquals("serializer cannot be null!", nullSerializerErrorStrategyBinaryEachEntityIsAValueInAHash.getMessage());
    }

    @Test
    void testInvalidDeserializer() {
        final var nullDeserializerErrorStrategyEachEntityIsAValue = assertThrows(IllegalArgumentException.class, () ->
                RedisRepositoryConfiguration.<Person>builder()
                        .jedisPool(jedisPool)
                        .collectionKey("people")
                        .strategyEachEntityIsAValue(RedisRepositoryStrategy.EachEntityIsAValue.<Person>builder()
                                .serializer(person -> "")
                                .build())
                        .build());
        assertEquals("deserializer cannot be null!", nullDeserializerErrorStrategyEachEntityIsAValue.getMessage());

        final var nullDeserializerErrorStrategyEachEntityIsAHash = assertThrows(IllegalArgumentException.class, () ->
                RedisRepositoryConfiguration.<Person>builder()
                        .jedisPool(jedisPool)
                        .collectionKey("people")
                        .strategyEachEntityIsAHash(RedisRepositoryStrategy.EachEntityIsAHash.<Person>builder()
                                .serializer(person -> Collections.emptyMap())
                                .build())
                        .build());
        assertEquals("deserializer cannot be null!", nullDeserializerErrorStrategyEachEntityIsAHash.getMessage());

        final var nullDeserializerErrorStrategyEachEntityIsAValueInAHash = assertThrows(IllegalArgumentException.class, () ->
                RedisRepositoryConfiguration.<Person>builder()
                        .jedisPool(jedisPool)
                        .collectionKey("people")
                        .strategyEachEntityIsAValueInAHash(RedisRepositoryStrategy.EachEntityIsAValueInAHash.<Person>builder()
                                .serializer(person -> "")
                                .build())
                        .build());
        assertEquals("deserializer cannot be null!", nullDeserializerErrorStrategyEachEntityIsAValueInAHash.getMessage());

        final var nullDeserializerErrorStrategyBinaryEachEntityIsAValue = assertThrows(IllegalArgumentException.class, () ->
                RedisRepositoryConfiguration.<Person>builder()
                        .jedisPool(jedisPool)
                        .collectionKey("people")
                        .strategyBinaryEachEntityIsAValue(RedisRepositoryStrategy.BinaryEachEntityIsAValue.<Person>builder()
                                .serializer(person -> new byte[0])
                                .build())
                        .build());
        assertEquals("deserializer cannot be null!", nullDeserializerErrorStrategyBinaryEachEntityIsAValue.getMessage());

        final var nullDeserializerErrorStrategyBinaryEachEntityIsAHash = assertThrows(IllegalArgumentException.class, () ->
                RedisRepositoryConfiguration.<Person>builder()
                        .jedisPool(jedisPool)
                        .collectionKey("people")
                        .strategyBinaryEachEntityIsAHash(RedisRepositoryStrategy.BinaryEachEntityIsAHash.<Person>builder()
                                .serializer(person -> Collections.emptyMap())
                                .build())
                        .build());
        assertEquals("deserializer cannot be null!", nullDeserializerErrorStrategyBinaryEachEntityIsAHash.getMessage());

        final var nullDeserializerErrorStrategyBinaryEachEntityIsAValueInAHash = assertThrows(IllegalArgumentException.class, () ->
                RedisRepositoryConfiguration.<Person>builder()
                        .jedisPool(jedisPool)
                        .collectionKey("people")
                        .strategyBinaryEachEntityIsAValueInAHash(RedisRepositoryStrategy.BinaryEachEntityIsAValueInAHash.<Person>builder()
                                .serializer(person -> new byte[0])
                                .build())
                        .build());
        assertEquals("deserializer cannot be null!", nullDeserializerErrorStrategyBinaryEachEntityIsAValueInAHash.getMessage());
    }

    private static String randomString() {
        return UUID.randomUUID().toString();
    }
}
