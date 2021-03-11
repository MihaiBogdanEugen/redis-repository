package com.github.mihaibogdaneugen.redisrepository;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.UncheckedIOException;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class StringRedisRepositoryTests extends RedisTestContainer {

    static Jedis jedis;
    static JedisPool jedisPool;
    static StringRedisRepository<Person> repository;

    @BeforeAll
    static void beforeAll() {
        jedisPool = new JedisPool(
                REDIS_CONTAINER.getContainerIpAddress(),
                REDIS_CONTAINER.getMappedPort(REDIS_PORT));
        jedis = jedisPool.getResource();
        repository = new StringRedisRepository<>(jedis, "people") {
            final ObjectMapper objectMapper = new ObjectMapper();

            @Override
            public String convertTo(final Person person) {
                try {
                    return objectMapper.writeValueAsString(person);
                } catch (final JsonProcessingException e) {
                    throw new UncheckedIOException(e);
                }
            }

            @Override
            public Person convertFrom(final String entity) {
                try {
                    return objectMapper.readValue(entity, Person.class);
                } catch (JsonProcessingException e) {
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
                new StringRedisRepository<Person>((Jedis) null, randomString()) {
                    @Override
                    public String convertTo(final Person entity) {
                        return null;
                    }

                    @Override
                    public Person convertFrom(final String entityAsString) {
                        return null;
                    }
                });
        assertEquals("jedis cannot be null!", nullJedisError.getMessage());
    }

    @Test
    void testNewInstanceWithNullJedisPool() {
        final var nullJedisPoolError = assertThrows(IllegalArgumentException.class, () ->
                new StringRedisRepository<Person>((JedisPool) null, randomString()) {
                    @Override
                    public String convertTo(final Person entity) {
                        return null;
                    }

                    @Override
                    public Person convertFrom(final String entityAsString) {
                        return null;
                    }
                });
        assertEquals("jedisPool cannot be null!", nullJedisPoolError.getMessage());
    }

    @Test
    void testNewInstanceWithValidJedisAndInvalidCollectionKey() {
        final var nullCollectionKeyError = assertThrows(IllegalArgumentException.class, () ->
                new StringRedisRepository<Person>(jedis, null) {
                    @Override
                    public String convertTo(final Person entity) {
                        return null;
                    }

                    @Override
                    public Person convertFrom(final String entityAsString) {
                        return null;
                    }
                });
        assertEquals("collectionKey cannot be null, nor empty!", nullCollectionKeyError.getMessage());

        final var emptyCollectionKeyError = assertThrows(IllegalArgumentException.class, () ->
                new StringRedisRepository<Person>(jedis, "") {
                    @Override
                    public String convertTo(final Person entity) {
                        return null;
                    }

                    @Override
                    public Person convertFrom(final String entityAsString) {
                        return null;
                    }
                });
        assertEquals("collectionKey cannot be null, nor empty!", emptyCollectionKeyError.getMessage());

        final var invalidCollectionKey = randomString() + ":" + randomString();
        final var invalidCollectionKeyError = assertThrows(IllegalArgumentException.class, () ->
                new StringRedisRepository<Person>(jedis, invalidCollectionKey) {
                    @Override
                    public String convertTo(final Person entity) {
                        return null;
                    }

                    @Override
                    public Person convertFrom(final String entityAsString) {
                        return null;
                    }
                });
        assertEquals("Collection key `" + invalidCollectionKey + "` cannot contain `:`", invalidCollectionKeyError.getMessage());
    }

    @Test
    void testNewInstanceWithValidJedisPoolAndInvalidCollectionKey() {
        final var nullCollectionKeyError = assertThrows(IllegalArgumentException.class, () ->
                new StringRedisRepository<Person>(jedisPool, null) {
                    @Override
                    public String convertTo(final Person entity) {
                        return null;
                    }

                    @Override
                    public Person convertFrom(final String entityAsString) {
                        return null;
                    }
                });
        assertEquals("collectionKey cannot be null, nor empty!", nullCollectionKeyError.getMessage());

        final var emptyCollectionKeyError = assertThrows(IllegalArgumentException.class, () ->
                new StringRedisRepository<Person>(jedisPool, "") {
                    @Override
                    public String convertTo(final Person entity) {
                        return null;
                    }

                    @Override
                    public Person convertFrom(final String entityAsString) {
                        return null;
                    }
                });
        assertEquals("collectionKey cannot be null, nor empty!", emptyCollectionKeyError.getMessage());

        final var invalidCollectionKey = randomString() + ":" + randomString();
        final var invalidCollectionKeyError = assertThrows(IllegalArgumentException.class, () ->
                new StringRedisRepository<Person>(jedisPool, invalidCollectionKey) {
                    @Override
                    public String convertTo(final Person entity) {
                        return null;
                    }

                    @Override
                    public Person convertFrom(final String entityAsString) {
                        return null;
                    }
                });
        assertEquals("Collection key `" + invalidCollectionKey + "` cannot contain `:`", invalidCollectionKeyError.getMessage());
    }

    private String randomString() {
        return UUID.randomUUID().toString();
    }
}
