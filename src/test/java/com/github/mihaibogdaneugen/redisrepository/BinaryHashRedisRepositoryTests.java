package com.github.mihaibogdaneugen.redisrepository;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.util.SafeEncoder;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class BinaryHashRedisRepositoryTests extends RedisTestContainer {

    static Jedis jedis;
    static JedisPool jedisPool;
    static BinaryHashRedisRepository<Person> repository;

    @BeforeAll
    static void beforeAll() {
        jedisPool = new JedisPool(
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

    @Test
    void testNewInstanceWithNullJedis() {
        final var nullJedisError = assertThrows(IllegalArgumentException.class, () ->
                new BinaryHashRedisRepository<Person>((Jedis) null, randomString()) {
                    @Override
                    public Map<byte[], byte[]> convertTo(final Person entity) {
                        return null;
                    }

                    @Override
                    public Person convertFrom(final Map<byte[], byte[]> entityAsMap) {
                        return null;
                    }
                });
        assertEquals("jedis cannot be null!", nullJedisError.getMessage());
    }

    @Test
    void testNewInstanceWithNullJedisPool() {
        final var nullJedisPoolError = assertThrows(IllegalArgumentException.class, () ->
                new BinaryHashRedisRepository<Person>((JedisPool) null, randomString()) {
                    @Override
                    public Map<byte[], byte[]> convertTo(final Person entity) {
                        return null;
                    }

                    @Override
                    public Person convertFrom(final Map<byte[], byte[]> entityAsMap) {
                        return null;
                    }
                });
        assertEquals("jedisPool cannot be null!", nullJedisPoolError.getMessage());
    }

    @Test
    void testNewInstanceWithValidJedisAndInvalidCollectionKey() {
        final var nullCollectionKeyError = assertThrows(IllegalArgumentException.class, () ->
                new BinaryHashRedisRepository<Person>(jedis, null) {
                    @Override
                    public Map<byte[], byte[]> convertTo(final Person entity) {
                        return null;
                    }

                    @Override
                    public Person convertFrom(final Map<byte[], byte[]> entityAsMap) {
                        return null;
                    }
                });
        assertEquals("collectionKey cannot be null, nor empty!", nullCollectionKeyError.getMessage());

        final var emptyCollectionKeyError = assertThrows(IllegalArgumentException.class, () ->
                new BinaryHashRedisRepository<Person>(jedis, "") {
                    @Override
                    public Map<byte[], byte[]> convertTo(final Person entity) {
                        return null;
                    }

                    @Override
                    public Person convertFrom(final Map<byte[], byte[]> entityAsMap) {
                        return null;
                    }
                });
        assertEquals("collectionKey cannot be null, nor empty!", emptyCollectionKeyError.getMessage());

        final var invalidCollectionKey = randomString() + ":" + randomString();
        final var invalidCollectionKeyError = assertThrows(IllegalArgumentException.class, () ->
                new BinaryHashRedisRepository<Person>(jedis, invalidCollectionKey) {
                    @Override
                    public Map<byte[], byte[]> convertTo(final Person entity) {
                        return null;
                    }

                    @Override
                    public Person convertFrom(final Map<byte[], byte[]> entityAsMap) {
                        return null;
                    }
                });
        assertEquals("Collection key `" + invalidCollectionKey + "` cannot contain `:`", invalidCollectionKeyError.getMessage());
    }

    @Test
    void testNewInstanceWithValidJedisPoolAndInvalidCollectionKey() {
        final var nullCollectionKeyError = assertThrows(IllegalArgumentException.class, () ->
                new BinaryHashRedisRepository<Person>(jedisPool, null) {
                    @Override
                    public Map<byte[], byte[]> convertTo(final Person entity) {
                        return null;
                    }

                    @Override
                    public Person convertFrom(final Map<byte[], byte[]> entityAsMap) {
                        return null;
                    }
                });
        assertEquals("collectionKey cannot be null, nor empty!", nullCollectionKeyError.getMessage());

        final var emptyCollectionKeyError = assertThrows(IllegalArgumentException.class, () ->
                new BinaryHashRedisRepository<Person>(jedisPool, "") {
                    @Override
                    public Map<byte[], byte[]> convertTo(final Person entity) {
                        return null;
                    }

                    @Override
                    public Person convertFrom(final Map<byte[], byte[]> entityAsMap) {
                        return null;
                    }
                });
        assertEquals("collectionKey cannot be null, nor empty!", emptyCollectionKeyError.getMessage());

        final var invalidCollectionKey = randomString() + ":" + randomString();
        final var invalidCollectionKeyError = assertThrows(IllegalArgumentException.class, () ->
                new BinaryHashRedisRepository<Person>(jedisPool, invalidCollectionKey) {
                    @Override
                    public Map<byte[], byte[]> convertTo(final Person entity) {
                        return null;
                    }

                    @Override
                    public Person convertFrom(final Map<byte[], byte[]> entityAsMap) {
                        return null;
                    }
                });
        assertEquals("Collection key `" + invalidCollectionKey + "` cannot contain `:`", invalidCollectionKeyError.getMessage());
    }

    private String randomString() {
        return UUID.randomUUID().toString();
    }
}
