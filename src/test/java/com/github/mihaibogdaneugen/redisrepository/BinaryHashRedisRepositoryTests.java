package com.github.mihaibogdaneugen.redisrepository;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
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
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
            public Map<byte[], byte[]> convertTo(final Person person) {
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
                fields.put(binaryEyeColor, SafeEncoder.encode(person.getEyeColor().name()));
                return fields;
            }

            @Override
            public Person convertFrom(final Map<byte[], byte[]> fields) {
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

    @BeforeEach
    void beforeEach() {
        jedis.flushAll();
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

    @Test
    void testGetInvalidArgument() {
        final var nullIdError = assertThrows(IllegalArgumentException.class, () ->
                repository.get((String) null));
        assertEquals("id cannot be null, nor empty!", nullIdError.getMessage());

        final var emptyIdError = assertThrows(IllegalArgumentException.class, () ->
                repository.get(""));
        assertEquals("id cannot be null, nor empty!", emptyIdError.getMessage());
    }

    @Test
    void testGetNonExistentEntity() {
        final var expectedPerson = Person.random();
        insert(expectedPerson);
        final var actualResult = repository.get(randomString());
        assertTrue(actualResult.isEmpty());
    }

    @Test
    void testGetEmptyEntity() {
        final var expectedPerson = Person.random();
        jedis.set("people:" + expectedPerson.getId(), "");
        final var actualResult = repository.get(randomString());
        assertTrue(actualResult.isEmpty());
    }

    @Test
    void testGet() {
        final var expectedPerson = Person.random();
        insert(expectedPerson);
        final var actualResult = repository.get(expectedPerson.getId());
        assertTrue(actualResult.isPresent());
        assertEquals(expectedPerson, actualResult.get());
    }

    @Test
    void testGetMultipleInvalidArgument() {
        final var nullIdError = assertThrows(IllegalArgumentException.class, () ->
                repository.get((String[]) null));
        assertEquals("ids cannot be null, nor empty!", nullIdError.getMessage());

        final var emptyIdError = assertThrows(IllegalArgumentException.class, () ->
                repository.get(new String[0]));
        assertEquals("ids cannot be null, nor empty!", emptyIdError.getMessage());
    }

    @Test
    void testGetMultipleExistingAndNonExistingEntities() {
        final var expectedPerson1 = Person.random();
        insert(expectedPerson1);
        final var expectedPerson2 = Person.random();
        insert(expectedPerson2);
        final var expectedPerson3 = Person.random();
        insert(expectedPerson3);
        final var actualResult = repository.get(expectedPerson1.getId(), randomString(), expectedPerson3.getId());
        assertEquals(2, actualResult.size());
        final var actualResultAsMap = actualResult.stream().collect(Collectors.toMap(Person::getId, x -> x));
        assertEquals(expectedPerson1, actualResultAsMap.get(expectedPerson1.getId()));
        assertEquals(expectedPerson3, actualResultAsMap.get(expectedPerson3.getId()));
    }

    @Test
    void testGetMultipleOnlySpecifiedEntities() {
        final var expectedPerson1 = Person.random();
        insert(expectedPerson1);
        final var expectedPerson2 = Person.random();
        insert(expectedPerson2);
        final var expectedPerson3 = Person.random();
        insert(expectedPerson3);
        final var unexpectedPerson1 = Person.random();
        insert(unexpectedPerson1);
        final var unexpectedPerson2 = Person.random();
        insert(unexpectedPerson2);
        final var actualResult = repository.get(expectedPerson1.getId(), expectedPerson2.getId(), expectedPerson3.getId());
        assertEquals(3, actualResult.size());
        final var actualResultAsMap = actualResult.stream().collect(Collectors.toMap(Person::getId, x -> x));
        assertEquals(expectedPerson1, actualResultAsMap.get(expectedPerson1.getId()));
        assertEquals(expectedPerson2, actualResultAsMap.get(expectedPerson2.getId()));
        assertEquals(expectedPerson3, actualResultAsMap.get(expectedPerson3.getId()));
    }

    @Test
    void testGetMultiple() {
        final var expectedPeopleMap = IntStream.range(0, 50)
                .mapToObj(i -> Person.random())
                .collect(Collectors.toMap(Person::getId, person -> person));
        expectedPeopleMap.values().forEach(this::insert);
        final var ids = expectedPeopleMap.keySet().toArray(String[]::new);
        final var actualResult = repository.get(ids);
        assertEquals(50, actualResult.size());
        final var actualPeopleMap = actualResult.stream().collect(Collectors.toMap(Person::getId, x -> x));
        expectedPeopleMap.forEach((key, value) -> assertEquals(value, actualPeopleMap.get(key)));
    }

    @Test
    void testGetAll() {
        final var expectedPeopleMap = IntStream.range(0, 50)
                .mapToObj(i -> Person.random())
                .collect(Collectors.toMap(Person::getId, person -> person));
        expectedPeopleMap.values().forEach(this::insert);
        final var actualResult = repository.getAll();
        assertEquals(50, actualResult.size());
        final var actualPeopleMap = actualResult.stream().collect(Collectors.toMap(Person::getId, x -> x));
        expectedPeopleMap.forEach((key, value) -> assertEquals(value, actualPeopleMap.get(key)));
    }

    @Test
    void testGetAll1000() {
        final var expectedPeopleMap = IntStream.range(0, 1000)
                .mapToObj(i -> Person.random())
                .collect(Collectors.toMap(Person::getId, person -> person));
        expectedPeopleMap.values().forEach(this::insert);
        final var actualResult = repository.getAll();
        assertEquals(1000, actualResult.size());
        final var actualPeopleMap = actualResult.stream().collect(Collectors.toMap(Person::getId, x -> x));
        expectedPeopleMap.forEach((key, value) -> assertEquals(value, actualPeopleMap.get(key)));
    }

    private void insert(final Person person) {
        jedis.hset(SafeEncoder.encode("people:" + person.getId()), repository.convertTo(person));
    }

    private String randomString() {
        return UUID.randomUUID().toString();
    }
}
