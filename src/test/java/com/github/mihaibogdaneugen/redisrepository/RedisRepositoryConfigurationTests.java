package com.github.mihaibogdaneugen.redisrepository;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.JedisPool;

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

    private static String randomString() {
        return UUID.randomUUID().toString();
    }
}
