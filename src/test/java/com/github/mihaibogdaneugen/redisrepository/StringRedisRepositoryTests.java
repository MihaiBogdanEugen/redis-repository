package com.github.mihaibogdaneugen.redisrepository;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.UncheckedIOException;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class StringRedisRepositoryTests extends RedisTestContainer {

    static Jedis jedis;
    static StringRedisRepository<Person> repository;

    @BeforeAll
    static void beforeAll() {
        final var jedisPool = new JedisPool(
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
}
