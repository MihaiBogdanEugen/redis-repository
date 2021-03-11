package com.github.mihaibogdaneugen.redisrepository;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

final class StringHashRedisRepositoryTests extends RedisTestContainer {

    static Jedis jedis;
    static StringHashRedisRepository<Person> repository;

    @BeforeAll
    static void beforeAll() {
        final var jedisPool = new JedisPool(
                REDIS_CONTAINER.getContainerIpAddress(),
                REDIS_CONTAINER.getMappedPort(REDIS_PORT));
        jedis = jedisPool.getResource();
        repository = new StringHashRedisRepository<>(jedis, "people") {

            @Override
            public Map<String, String> convertTo(Person person) {
                final var fields = new HashMap<String, String>();
                Optional.ofNullable(person.getId())
                        .ifPresent(value -> fields.put("id", value));
                Optional.ofNullable(person.getFullName())
                        .ifPresent(value -> fields.put("fullName", value));
                Optional.ofNullable(person.getDateOfBirth())
                        .ifPresent(value -> fields.put("dateOfBirth", value.format(DateTimeFormatter.ISO_LOCAL_DATE)));
                fields.put("isMarried", Boolean.toString(person.isMarried()));
                if (person.getHeightMeters() > 0) {
                    fields.put("heightMeters", Float.toString(person.getHeightMeters()));
                }
                if (person.getEyeColor() != Person.EyeColor.UNKNOWN) {
                    fields.put("eyeColor", person.getEyeColor().name());
                }
                return fields;
            }

            @Override
            public Person convertFrom(Map<String, String> fields) {
                final var person = new Person();
                Optional.ofNullable(fields.get("id"))
                        .ifPresent(person::setId);
                Optional.ofNullable(fields.get("fullName"))
                        .ifPresent(person::setFullName);
                Optional.ofNullable(fields.get("dateOfBirth"))
                        .ifPresent(value -> person.setDateOfBirth(LocalDate.parse(value, DateTimeFormatter.ISO_LOCAL_DATE)));
                person.setMarried(Boolean.parseBoolean(fields.get("isMarried")));
                Optional.ofNullable(fields.get("heightMeters"))
                        .ifPresent(value -> person.setHeightMeters(Float.parseFloat(value)));
                Optional.ofNullable(fields.get("eyeColor"))
                        .ifPresent(value -> person.setEyeColor(Person.EyeColor.valueOf(value)));
                return person;
            }
        };
    }

    @AfterAll
    static void afterAll() {
        repository.close();
    }
}
