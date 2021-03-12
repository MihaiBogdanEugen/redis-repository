package com.github.mihaibogdaneugen.redisrepository;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.UncheckedIOException;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

final class FieldStringHashRedisRepositoryTests extends RedisTestContainer {

    static Jedis jedis;
    static JedisPool jedisPool;
    static FieldStringHashRedisRepository<Person> repository;

    @BeforeAll
    static void beforeAll() {
        jedisPool = new JedisPool(
                REDIS_CONTAINER.getContainerIpAddress(),
                REDIS_CONTAINER.getMappedPort(REDIS_PORT));
        jedis = jedisPool.getResource();
        repository = new FieldStringHashRedisRepository<>(jedis, "people") {
            final ObjectMapper objectMapper = new ObjectMapper()
                    .registerModule(new JavaTimeModule())
                    .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

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

    @BeforeEach
    void beforeEach() {
        jedis.flushAll();
    }

    @Test
    void testNewInstanceWithNullJedis() {
        final var nullJedisError = assertThrows(IllegalArgumentException.class, () ->
                new FieldStringHashRedisRepository<Person>((Jedis) null, randomString()) {
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
                new FieldStringHashRedisRepository<Person>((JedisPool) null, randomString()) {
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
                new FieldStringHashRedisRepository<Person>(jedis, null) {
                    @Override
                    public String convertTo(final Person entity) {
                        return null;
                    }

                    @Override
                    public Person convertFrom(final String entityAsString) {
                        return null;
                    }
                });
        assertEquals("parentKey cannot be null, nor empty!", nullCollectionKeyError.getMessage());

        final var emptyCollectionKeyError = assertThrows(IllegalArgumentException.class, () ->
                new FieldStringHashRedisRepository<Person>(jedis, "") {
                    @Override
                    public String convertTo(final Person entity) {
                        return null;
                    }

                    @Override
                    public Person convertFrom(final String entityAsString) {
                        return null;
                    }
                });
        assertEquals("parentKey cannot be null, nor empty!", emptyCollectionKeyError.getMessage());

        final var invalidCollectionKey = randomString() + ":" + randomString();
        final var invalidCollectionKeyError = assertThrows(IllegalArgumentException.class, () ->
                new FieldStringHashRedisRepository<Person>(jedis, invalidCollectionKey) {
                    @Override
                    public String convertTo(final Person entity) {
                        return null;
                    }

                    @Override
                    public Person convertFrom(final String entityAsString) {
                        return null;
                    }
                });
        assertEquals("Parent key `" + invalidCollectionKey + "` cannot contain `:`, nor `_lock`!", invalidCollectionKeyError.getMessage());
    }

    @Test
    void testNewInstanceWithValidJedisPoolAndInvalidCollectionKey() {
        final var nullCollectionKeyError = assertThrows(IllegalArgumentException.class, () ->
                new FieldStringHashRedisRepository<Person>(jedisPool, null) {
                    @Override
                    public String convertTo(final Person entity) {
                        return null;
                    }

                    @Override
                    public Person convertFrom(final String entityAsString) {
                        return null;
                    }
                });
        assertEquals("parentKey cannot be null, nor empty!", nullCollectionKeyError.getMessage());

        final var emptyCollectionKeyError = assertThrows(IllegalArgumentException.class, () ->
                new FieldStringHashRedisRepository<Person>(jedisPool, "") {
                    @Override
                    public String convertTo(final Person entity) {
                        return null;
                    }

                    @Override
                    public Person convertFrom(final String entityAsString) {
                        return null;
                    }
                });
        assertEquals("parentKey cannot be null, nor empty!", emptyCollectionKeyError.getMessage());

        final var invalidCollectionKey = randomString() + ":" + randomString();
        final var invalidCollectionKeyError = assertThrows(IllegalArgumentException.class, () ->
                new FieldStringHashRedisRepository<Person>(jedisPool, invalidCollectionKey) {
                    @Override
                    public String convertTo(final Person entity) {
                        return null;
                    }

                    @Override
                    public Person convertFrom(final String entityAsString) {
                        return null;
                    }
                });
        assertEquals("Parent key `" + invalidCollectionKey + "` cannot contain `:`, nor `_lock`!", invalidCollectionKeyError.getMessage());
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
        jedis.hset("people", person.getId(), repository.convertTo(person));
    }

    private String randomString() {
        return UUID.randomUUID().toString();
    }
}
