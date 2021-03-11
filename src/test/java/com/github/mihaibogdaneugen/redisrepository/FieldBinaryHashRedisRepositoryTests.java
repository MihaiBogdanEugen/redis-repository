package com.github.mihaibogdaneugen.redisrepository;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.util.SafeEncoder;

import java.io.UncheckedIOException;

final class FieldBinaryHashRedisRepositoryTests extends RedisTestContainer {

    static Jedis jedis;
    static FieldBinaryHashRedisRepository<Person> repository;

    @BeforeAll
    static void beforeAll() {
        final var jedisPool = new JedisPool(
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
}
