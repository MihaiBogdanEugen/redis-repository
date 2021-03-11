package com.github.mihaibogdaneugen.redisrepository;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.util.SafeEncoder;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

final class BinaryHashRedisRepositoryTests extends RedisTestContainer {

    static Jedis jedis;
    static BinaryHashRedisRepository<Person> repository;

    @BeforeAll
    static void beforeAll() {
        final var jedisPool = new JedisPool(
                REDIS_CONTAINER.getContainerIpAddress(),
                REDIS_CONTAINER.getMappedPort(REDIS_PORT));
        jedis = jedisPool.getResource();
        repository = new BinaryHashRedisRepository<>(jedis, "people") {
            private final byte[] binaryId = SafeEncoder.encode("id");
            private final byte[] binaryFullName = SafeEncoder.encode("fullName");
            private final byte[] binaryDateOfBirth = SafeEncoder.encode("dateOfBirth");
            private final byte[] binaryIsMarried = SafeEncoder.encode("isMarried");
            private final byte[] binaryHeightMeters = SafeEncoder.encode("heightMeters");
            private final byte[] binaryEyeColor = SafeEncoder.encode("eyeColor");

            @Override
            public Map<byte[], byte[]> convertTo(Person person) {
                final var fields = new HashMap<byte[], byte[]>();
                Optional.ofNullable(person.getId())
                        .ifPresent(value -> fields.put(binaryId, SafeEncoder.encode(value)));
                Optional.ofNullable(person.getFullName())
                        .ifPresent(value -> fields.put(binaryFullName, SafeEncoder.encode(value)));
                Optional.ofNullable(person.getDateOfBirth())
                        .ifPresent(value -> fields.put(binaryDateOfBirth, SafeEncoder.encode(value.format(DateTimeFormatter.ISO_LOCAL_DATE))));
                fields.put(binaryIsMarried, SafeEncoder.encode(Boolean.toString(person.isMarried())));
                if (person.getHeightMeters() > 0) {
                    fields.put(binaryHeightMeters, SafeEncoder.encode(Float.toString(person.getHeightMeters())));
                }
                if (person.getEyeColor() != Person.EyeColor.UNKNOWN) {
                    fields.put(binaryEyeColor, SafeEncoder.encode(person.getEyeColor().name()));
                }
                return fields;
            }

            @Override
            public Person convertFrom(Map<byte[], byte[]> fields) {
                final var person = new Person();
                Optional.ofNullable(fields.get(binaryId))
                        .map(SafeEncoder::encode)
                        .ifPresent(person::setId);
                Optional.ofNullable(fields.get(binaryFullName))
                        .map(SafeEncoder::encode)
                        .ifPresent(person::setFullName);
                Optional.ofNullable(fields.get(binaryDateOfBirth))
                        .map(SafeEncoder::encode)
                        .ifPresent(value -> person.setDateOfBirth(LocalDate.parse(value, DateTimeFormatter.ISO_LOCAL_DATE)));
                person.setMarried(Boolean.parseBoolean(SafeEncoder.encode(fields.get(binaryIsMarried))));
                Optional.ofNullable(fields.get(binaryHeightMeters))
                        .map(SafeEncoder::encode)
                        .ifPresent(value -> person.setHeightMeters(Float.parseFloat(value)));
                Optional.ofNullable(fields.get(binaryEyeColor))
                        .map(SafeEncoder::encode)
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
