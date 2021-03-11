package com.github.mihaibogdaneugen.redisrepository;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.util.SafeEncoder;

import java.io.UncheckedIOException;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class FieldBinaryHashRedisRepositoryTests extends RedisTestContainer {

    static Jedis jedis;
    static JedisPool jedisPool;
    static FieldBinaryHashRedisRepository<Person> repository;

    @BeforeAll
    static void beforeAll() {
        jedisPool = new JedisPool(
                REDIS_CONTAINER.getContainerIpAddress(),
                REDIS_CONTAINER.getMappedPort(REDIS_PORT));
        jedis = jedisPool.getResource();
        repository = new FieldBinaryHashRedisRepository<>(jedis, "people") {
            private final ObjectMapper objectMapper = new ObjectMapper();

            @Override
            public byte[] convertTo(final Person person) {
                try {
                    return SafeEncoder.encode(objectMapper.writeValueAsString(person));
                } catch (final JsonProcessingException e) {
                    throw new UncheckedIOException(e);
                }
            }

            @Override
            public Person convertFrom(final byte[] entity) {
                try {
                    return objectMapper.readValue(SafeEncoder.encode(entity), Person.class);
                } catch (final JsonProcessingException e) {
                    throw new UncheckedIOException(e);
                }
            }
        };
    }

    @AfterAll
    static void afterAll() {
        repository.close();
    }

    @Test
    void testNewInstanceWithNullJedis() {
        final var nullJedisError = assertThrows(IllegalArgumentException.class, () ->
                new FieldBinaryHashRedisRepository<Person>((Jedis) null, randomString()) {
                    @Override
                    public byte[] convertTo(final Person entity) {
                        return null;
                    }

                    @Override
                    public Person convertFrom(final byte[] entityAsBytes) {
                        return null;
                    }
                });
        assertEquals("jedis cannot be null!", nullJedisError.getMessage());
    }

    @Test
    void testNewInstanceWithNullJedisPool() {
        final var nullJedisPoolError = assertThrows(IllegalArgumentException.class, () ->
                new FieldBinaryHashRedisRepository<Person>((JedisPool) null, randomString()) {
                    @Override
                    public byte[] convertTo(final Person entity) {
                        return null;
                    }

                    @Override
                    public Person convertFrom(final byte[] entityAsBytes) {
                        return null;
                    }
                });
        assertEquals("jedisPool cannot be null!", nullJedisPoolError.getMessage());
    }

    @Test
    void testNewInstanceWithValidJedisAndInvalidCollectionKey() {
        final var nullCollectionKeyError = assertThrows(IllegalArgumentException.class, () ->
                new FieldBinaryHashRedisRepository<Person>(jedis, null) {
                    @Override
                    public byte[] convertTo(final Person entity) {
                        return null;
                    }

                    @Override
                    public Person convertFrom(final byte[] entityAsBytes) {
                        return null;
                    }
                });
        assertEquals("parentKey cannot be null, nor empty!", nullCollectionKeyError.getMessage());

        final var emptyCollectionKeyError = assertThrows(IllegalArgumentException.class, () ->
                new FieldBinaryHashRedisRepository<Person>(jedis, "") {
                    @Override
                    public byte[] convertTo(final Person entity) {
                        return null;
                    }

                    @Override
                    public Person convertFrom(final byte[] entityAsBytes) {
                        return null;
                    }
                });
        assertEquals("parentKey cannot be null, nor empty!", emptyCollectionKeyError.getMessage());

        final var invalidCollectionKey = randomString() + ":" + randomString();
        final var invalidCollectionKeyError = assertThrows(IllegalArgumentException.class, () ->
                new FieldBinaryHashRedisRepository<Person>(jedis, invalidCollectionKey) {
                    @Override
                    public byte[] convertTo(final Person entity) {
                        return null;
                    }

                    @Override
                    public Person convertFrom(final byte[] entityAsBytes) {
                        return null;
                    }
                });
        assertEquals("Parent key `" + invalidCollectionKey + "` cannot contain `:`, nor `_lock`!", invalidCollectionKeyError.getMessage());
    }

    @Test
    void testNewInstanceWithValidJedisPoolAndInvalidCollectionKey() {
        final var nullCollectionKeyError = assertThrows(IllegalArgumentException.class, () ->
                new FieldBinaryHashRedisRepository<Person>(jedisPool, null) {
                    @Override
                    public byte[] convertTo(final Person entity) {
                        return null;
                    }

                    @Override
                    public Person convertFrom(final byte[] entityAsBytes) {
                        return null;
                    }
                });
        assertEquals("parentKey cannot be null, nor empty!", nullCollectionKeyError.getMessage());

        final var emptyCollectionKeyError = assertThrows(IllegalArgumentException.class, () ->
                new FieldBinaryHashRedisRepository<Person>(jedisPool, "") {
                    @Override
                    public byte[] convertTo(final Person entity) {
                        return null;
                    }

                    @Override
                    public Person convertFrom(final byte[] entityAsBytes) {
                        return null;
                    }
                });
        assertEquals("parentKey cannot be null, nor empty!", emptyCollectionKeyError.getMessage());

        final var invalidCollectionKey = randomString() + ":" + randomString();
        final var invalidCollectionKeyError = assertThrows(IllegalArgumentException.class, () ->
                new FieldBinaryHashRedisRepository<Person>(jedisPool, invalidCollectionKey) {
                    @Override
                    public byte[] convertTo(final Person entity) {
                        return null;
                    }

                    @Override
                    public Person convertFrom(final byte[] entityAsBytes) {
                        return null;
                    }
                });
        assertEquals("Parent key `" + invalidCollectionKey + "` cannot contain `:`, nor `_lock`!", invalidCollectionKeyError.getMessage());
    }

    private String randomString() {
        return UUID.randomUUID().toString();
    }
}
