package com.github.mihaibogdaneugen.redisrepository;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.github.mihaibogdaneugen.redisrepository.BaseRedisRepository.isNullOrEmpty;
import static org.junit.jupiter.api.Assertions.*;

final class StringHashRedisRepositoryTests extends RedisTestContainer {

    static Jedis jedis;
    static JedisPool jedisPool;
    static StringHashRedisRepository<Person> repository;

    @BeforeAll
    static void beforeAll() {
        jedisPool = new JedisPool(
                REDIS_CONTAINER.getContainerIpAddress(),
                REDIS_CONTAINER.getMappedPort(REDIS_PORT));
        jedis = jedisPool.getResource();
        repository = new StringHashRedisRepository<>(jedis, "people") {

            @Override
            public Map<String, String> convertTo(final Person person) {
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
                fields.put("eyeColor", person.getEyeColor().name());
                return fields;
            }

            @Override
            public Person convertFrom(final Map<String, String> fields) {
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

    @BeforeEach
    void beforeEach() {
        jedis.flushAll();
    }

    @Test
    void testNewInstanceWithNullJedis() {
        final var nullJedisError = assertThrows(IllegalArgumentException.class, () ->
                new StringHashRedisRepository<Person>((Jedis) null, randomString()) {
                    @Override
                    public Map<String, String> convertTo(final Person entity) {
                        return null;
                    }

                    @Override
                    public Person convertFrom(final Map<String, String> entityAsMap) {
                        return null;
                    }
                });
        assertEquals("jedis cannot be null!", nullJedisError.getMessage());
    }

    @Test
    void testNewInstanceWithNullJedisPool() {
        final var nullJedisPoolError = assertThrows(IllegalArgumentException.class, () ->
                new StringHashRedisRepository<Person>((JedisPool) null, randomString()) {
                    @Override
                    public Map<String, String> convertTo(final Person entity) {
                        return null;
                    }

                    @Override
                    public Person convertFrom(final Map<String, String> entityAsMap) {
                        return null;
                    }
                });
        assertEquals("jedisPool cannot be null!", nullJedisPoolError.getMessage());
    }

    @Test
    void testNewInstanceWithValidJedisAndInvalidCollectionKey() {
        final var nullCollectionKeyError = assertThrows(IllegalArgumentException.class, () ->
                new StringHashRedisRepository<Person>(jedis, null) {
                    @Override
                    public Map<String, String> convertTo(final Person entity) {
                        return null;
                    }

                    @Override
                    public Person convertFrom(final Map<String, String> entityAsMap) {
                        return null;
                    }
                });
        assertEquals("collectionKey cannot be null, nor empty!", nullCollectionKeyError.getMessage());

        final var emptyCollectionKeyError = assertThrows(IllegalArgumentException.class, () ->
                new StringHashRedisRepository<Person>(jedis, "") {
                    @Override
                    public Map<String, String> convertTo(final Person entity) {
                        return null;
                    }

                    @Override
                    public Person convertFrom(final Map<String, String> entityAsMap) {
                        return null;
                    }
                });
        assertEquals("collectionKey cannot be null, nor empty!", emptyCollectionKeyError.getMessage());

        final var invalidCollectionKey = randomString() + ":" + randomString();
        final var invalidCollectionKeyError = assertThrows(IllegalArgumentException.class, () ->
                new StringHashRedisRepository<Person>(jedis, invalidCollectionKey) {
                    @Override
                    public Map<String, String> convertTo(final Person entity) {
                        return null;
                    }

                    @Override
                    public Person convertFrom(final Map<String, String> entityAsMap) {
                        return null;
                    }
                });
        assertEquals("Collection key `" + invalidCollectionKey + "` cannot contain `:`", invalidCollectionKeyError.getMessage());
    }

    @Test
    void testNewInstanceWithValidJedisPoolAndInvalidCollectionKey() {
        final var nullCollectionKeyError = assertThrows(IllegalArgumentException.class, () ->
                new StringHashRedisRepository<Person>(jedisPool, null) {
                    @Override
                    public Map<String, String> convertTo(final Person entity) {
                        return null;
                    }

                    @Override
                    public Person convertFrom(final Map<String, String> entityAsMap) {
                        return null;
                    }
                });
        assertEquals("collectionKey cannot be null, nor empty!", nullCollectionKeyError.getMessage());

        final var emptyCollectionKeyError = assertThrows(IllegalArgumentException.class, () ->
                new StringHashRedisRepository<Person>(jedisPool, "") {
                    @Override
                    public Map<String, String> convertTo(final Person entity) {
                        return null;
                    }

                    @Override
                    public Person convertFrom(final Map<String, String> entityAsMap) {
                        return null;
                    }
                });
        assertEquals("collectionKey cannot be null, nor empty!", emptyCollectionKeyError.getMessage());

        final var invalidCollectionKey = randomString() + ":" + randomString();
        final var invalidCollectionKeyError = assertThrows(IllegalArgumentException.class, () ->
                new StringHashRedisRepository<Person>(jedisPool, invalidCollectionKey) {
                    @Override
                    public Map<String, String> convertTo(final Person entity) {
                        return null;
                    }

                    @Override
                    public Person convertFrom(final Map<String, String> entityAsMap) {
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

    @Test
    void testExistsInvalidArgument() {
        final var nullIdError = assertThrows(IllegalArgumentException.class, () ->
                repository.exists(null));
        assertEquals("id cannot be null, nor empty!", nullIdError.getMessage());

        final var emptyIdError = assertThrows(IllegalArgumentException.class, () ->
                repository.exists(""));
        assertEquals("id cannot be null, nor empty!", emptyIdError.getMessage());
    }

    @Test
    void testExists() {
        final var expectedPerson1 = Person.random();
        insert(expectedPerson1);
        final var expectedPerson2 = Person.random();
        assertTrue(repository.exists(expectedPerson1.getId()));
        assertFalse(repository.exists(expectedPerson2.getId()));
    }

    @Test
    void testDeleteInvalidArgument() {
        final var nullIdError = assertThrows(IllegalArgumentException.class, () ->
                repository.delete((String)null));
        assertEquals("id cannot be null, nor empty!", nullIdError.getMessage());

        final var emptyIdError = assertThrows(IllegalArgumentException.class, () ->
                repository.delete(""));
        assertEquals("id cannot be null, nor empty!", emptyIdError.getMessage());
    }

    @Test
    void testDeleteNonExistentEntity() {
        final var expectedPerson1 = Person.random();
        insert(expectedPerson1);
        final var expectedPerson2 = Person.random();
        repository.delete(expectedPerson2.getId());
        final var actualPerson1 = get(expectedPerson1.getId());
        assertTrue(actualPerson1.isPresent());
        assertEquals(expectedPerson1, actualPerson1.get());
        final var actualPerson2 = get(expectedPerson2.getId());
        assertTrue(actualPerson2.isEmpty());
    }

    @Test
    void testDelete() {
        final var expectedPerson1 = Person.random();
        insert(expectedPerson1);
        repository.delete(expectedPerson1.getId());
        final var actualPerson1 = get(expectedPerson1.getId());
        assertTrue(actualPerson1.isEmpty());
    }

    @Test
    void testDeleteMultipleInvalidArgument() {
        final var nullIdError = assertThrows(IllegalArgumentException.class, () ->
                repository.delete((String[])null));
        assertEquals("ids cannot be null, nor empty!", nullIdError.getMessage());

        @SuppressWarnings("RedundantArrayCreation")
        final var emptyIdError = assertThrows(IllegalArgumentException.class, () ->
                repository.delete(new String[0]));
        assertEquals("ids cannot be null, nor empty!", emptyIdError.getMessage());
    }

    @Test
    void testDeleteMultipleExistingAndNonExistingEntities() {
        final var expectedPerson1 = Person.random();
        insert(expectedPerson1);
        final var expectedPerson2 = Person.random();
        insert(expectedPerson2);
        final var expectedPerson3 = Person.random();
        insert(expectedPerson3);
        repository.delete(expectedPerson1.getId(), randomString(), expectedPerson3.getId());
        final var actualPerson1 = get(expectedPerson1.getId());
        final var actualPerson2 = get(expectedPerson2.getId());
        final var actualPerson3 = get(expectedPerson3.getId());
        assertTrue(actualPerson1.isEmpty());
        assertTrue(actualPerson2.isPresent());
        assertEquals(expectedPerson2, actualPerson2.get());
        assertTrue(actualPerson3.isEmpty());
    }

    @Test
    void testDeleteMultipleOnlySpecifiedEntities() {
        final var expectedPerson1 = Person.random();
        insert(expectedPerson1);
        final var expectedPerson2 = Person.random();
        insert(expectedPerson2);
        final var expectedPerson3 = Person.random();
        insert(expectedPerson3);
        final var expectedPerson4 = Person.random();
        insert(expectedPerson4);
        final var expectedPerson5 = Person.random();
        insert(expectedPerson5);
        repository.delete(expectedPerson1.getId(), expectedPerson2.getId(), expectedPerson3.getId());
        final var actualPerson1 = get(expectedPerson1.getId());
        final var actualPerson2 = get(expectedPerson2.getId());
        final var actualPerson3 = get(expectedPerson3.getId());
        final var actualPerson4 = get(expectedPerson4.getId());
        final var actualPerson5 = get(expectedPerson5.getId());
        assertTrue(actualPerson1.isEmpty());
        assertTrue(actualPerson2.isEmpty());
        assertTrue(actualPerson3.isEmpty());
        assertTrue(actualPerson4.isPresent());
        assertEquals(expectedPerson4, actualPerson4.get());
        assertTrue(actualPerson5.isPresent());
        assertEquals(expectedPerson5, actualPerson5.get());
    }

    @Test
    void testDeleteMultiple() {
        final var expectedPeopleMap = IntStream.range(0, 50)
                .mapToObj(i -> Person.random())
                .collect(Collectors.toMap(Person::getId, person -> person));
        expectedPeopleMap.values().forEach(this::insert);
        repository.delete(expectedPeopleMap.keySet().toArray(String[]::new));
        expectedPeopleMap.keySet().forEach(key -> {
            final var result = get(key);
            assertTrue(result.isEmpty());
        });
    }

    @Test
    void testDeleteAll() {
        final var expectedPeopleMap = IntStream.range(0, 50)
                .mapToObj(i -> Person.random())
                .collect(Collectors.toMap(Person::getId, person -> person));
        expectedPeopleMap.values().forEach(this::insert);
        repository.deleteAll();
        expectedPeopleMap.keySet().forEach(key -> {
            final var result = get(key);
            assertTrue(result.isEmpty());
        });
    }

    @Test
    void testDeleteAll1000() {
        final var expectedPeopleMap = IntStream.range(0, 1000)
                .mapToObj(i -> Person.random())
                .collect(Collectors.toMap(Person::getId, person -> person));
        expectedPeopleMap.values().forEach(this::insert);
        repository.deleteAll();
        expectedPeopleMap.keySet().forEach(key -> {
            final var result = get(key);
            assertTrue(result.isEmpty());
        });
    }

    private void insert(final Person person) {
        jedis.hset("people:" + person.getId(), repository.convertTo(person));
    }

    private Optional<Person> get(final String id) {
        final var entity = jedis.hgetAll("people:" + id);
        return isNullOrEmpty(entity) ? Optional.empty() : Optional.of(repository.convertFrom(entity));
    }

    private String randomString() {
        return UUID.randomUUID().toString();
    }
}
