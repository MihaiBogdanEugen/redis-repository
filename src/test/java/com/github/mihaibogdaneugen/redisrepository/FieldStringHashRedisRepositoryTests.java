package com.github.mihaibogdaneugen.redisrepository;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.UncheckedIOException;

final class FieldStringHashRedisRepositoryTests extends RedisTestContainer {

    static Jedis jedis;
    static FieldStringHashRedisRepository<Person> repository;

    @BeforeAll
    static void beforeAll() {
        final var jedisPool = new JedisPool(
                REDIS_CONTAINER.getContainerIpAddress(),
                REDIS_CONTAINER.getMappedPort(REDIS_PORT));
        jedis = jedisPool.getResource();
        repository = new FieldStringHashRedisRepository<>(jedis, "people") {
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
