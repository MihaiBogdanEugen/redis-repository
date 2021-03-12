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
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.github.mihaibogdaneugen.redisrepository.BaseRedisRepository.isNullOrEmptyOrBlank;
import static org.junit.jupiter.api.Assertions.*;

final class StringValueRedisRepositoryTests extends RedisTestContainer {

    static Jedis jedis;
    static JedisPool jedisPool;
    static StringValueRedisRepository<Person> repository;

    @BeforeAll
    static void beforeAll() {
        jedisPool = new JedisPool(
                REDIS_CONTAINER.getContainerIpAddress(),
                REDIS_CONTAINER.getMappedPort(REDIS_PORT));
        jedis = jedisPool.getResource();
        repository = new StringValueRedisRepository<>(jedis, "people") {
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

    @BeforeEach
    void beforeEach() {
        jedis.flushAll();
    }

    @Test
    void testNewInstanceWithNullJedis() {
        final var nullJedisError = assertThrows(IllegalArgumentException.class, () ->
                new StringValueRedisRepository<Person>((Jedis) null, randomString()) {
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
                new StringValueRedisRepository<Person>((JedisPool) null, randomString()) {
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
                new StringValueRedisRepository<Person>(jedis, null) {
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
                new StringValueRedisRepository<Person>(jedis, "") {
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
                new StringValueRedisRepository<Person>(jedis, invalidCollectionKey) {
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
                new StringValueRedisRepository<Person>(jedisPool, null) {
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
                new StringValueRedisRepository<Person>(jedisPool, "") {
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
                new StringValueRedisRepository<Person>(jedisPool, invalidCollectionKey) {
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
    void testSetInvalidArgumentId() {
        final var person = Person.random();
        final var nullIdError = assertThrows(IllegalArgumentException.class, () ->
                repository.set(null, person));
        assertEquals("id cannot be null, nor empty!", nullIdError.getMessage());

        final var emptyIdError = assertThrows(IllegalArgumentException.class, () ->
                repository.set("", person));
        assertEquals("id cannot be null, nor empty!", emptyIdError.getMessage());
    }

    @Test
    void testSetNullArgumentEntity() {
        final var person = Person.random();
        final var nullPersonError = assertThrows(IllegalArgumentException.class, () ->
                repository.set(person.getId(), null));
        assertEquals("entity cannot be null!", nullPersonError.getMessage());
    }

    @Test
    void testSetReplace() {
        final var oldPerson = Person.random();
        insert(oldPerson);
        final var expectedPerson = Person.random();
        expectedPerson.setId(oldPerson.getId());
        repository.set(expectedPerson.getId(), expectedPerson);
        final var actualPerson = get(expectedPerson.getId());
        assertTrue(actualPerson.isPresent());
        assertEquals(expectedPerson, actualPerson.get());
    }

    @Test
    void testSetInsert() {
        final var expectedPerson = Person.random();
        repository.set(expectedPerson.getId(), expectedPerson);
        final var actualPerson = get(expectedPerson.getId());
        assertTrue(actualPerson.isPresent());
        assertEquals(expectedPerson, actualPerson.get());
    }

    @Test
    void testSetIfNotExistsInvalidArgumentId() {
        final var person = Person.random();
        final var nullIdError = assertThrows(IllegalArgumentException.class, () ->
                repository.setIfNotExist(null, person));
        assertEquals("id cannot be null, nor empty!", nullIdError.getMessage());

        final var emptyIdError = assertThrows(IllegalArgumentException.class, () ->
                repository.setIfNotExist("", person));
        assertEquals("id cannot be null, nor empty!", emptyIdError.getMessage());
    }

    @Test
    void testSetIfNotExistsNullArgumentEntity() {
        final var person = Person.random();
        final var nullPersonError = assertThrows(IllegalArgumentException.class, () ->
                repository.setIfNotExist(person.getId(), null));
        assertEquals("entity cannot be null!", nullPersonError.getMessage());
    }

    @Test
    void testSetIfNotExistsReplace() {
        final var oldPerson = Person.random();
        insert(oldPerson);
        final var expectedPerson = Person.random();
        expectedPerson.setId(oldPerson.getId());
        repository.setIfNotExist(expectedPerson.getId(), expectedPerson);
        final var actualPerson = get(expectedPerson.getId());
        assertTrue(actualPerson.isPresent());
        assertEquals(oldPerson, actualPerson.get());
    }

    @Test
    void testSetIfNotExists() {
        final var expectedPerson = Person.random();
        repository.setIfNotExist(expectedPerson.getId(), expectedPerson);
        final var actualPerson = get(expectedPerson.getId());
        assertTrue(actualPerson.isPresent());
        assertEquals(expectedPerson, actualPerson.get());
    }

    @Test
    void testUpdateInvalidArgumentId() {
        final var nullIdError = assertThrows(IllegalArgumentException.class, () ->
                repository.update(null, person -> null));
        assertEquals("id cannot be null, nor empty!", nullIdError.getMessage());

        final var emptyIdError = assertThrows(IllegalArgumentException.class, () ->
                repository.update("", person -> null));
        assertEquals("id cannot be null, nor empty!", emptyIdError.getMessage());
    }

    @Test
    void testUpdateNullUpdater() {
        final var nullIdError = assertThrows(IllegalArgumentException.class, () ->
                repository.update(randomString(), null));
        assertEquals("updater cannot be null!", nullIdError.getMessage());
    }

    @Test
    void testUpdateNonExistingEntity() {
        final var person = Person.random();
        final var updateResult = repository.update(person.getId(), x -> new Person(
                x.getId(),
                randomString(),
                x.getDateOfBirth(),
                x.isMarried(),
                x.getHeightMeters(),
                x.getEyeColor()
        ));
        assertTrue(updateResult.isEmpty());
    }

    @Test
    void testUpdate() {
        final var expectedPerson = Person.random();
        insert(expectedPerson);
        final var newFullName = randomString();
        final var newHeightMeters = (150 + new Random().nextInt(50)) / 100f;
        final var updater = new Function<Person, Person>() {
            @Override
            public Person apply(final Person x) {
                return new Person(
                        x.getId(),
                        newFullName,
                        x.getDateOfBirth(),
                        x.isMarried(),
                        newHeightMeters,
                        x.getEyeColor());
            }
        };
        final var updateResult = repository.update(expectedPerson.getId(), updater);
        assertTrue(updateResult.isPresent());
        assertTrue(updateResult.get());
        final var getResult = get(expectedPerson.getId());
        assertTrue(getResult.isPresent());
        assertNotEquals(expectedPerson, getResult.get());
        expectedPerson.setFullName(newFullName);
        expectedPerson.setHeightMeters(newHeightMeters);
        assertEquals(expectedPerson, getResult.get());
    }

    @Test
    void testUpdateTransactionalBehaviour() {
        final var expectedPerson = Person.random();
        insert(expectedPerson);
        final var newFullName = randomString();
        final var newFullName2 = randomString();
        final var newHeightMeters = (150 + new Random().nextInt(50)) / 100f;
        final var updater = new Function<Person, Person>() {
            @Override
            public Person apply(final Person x) {
                try {
                    Thread.sleep(1000);
                } catch (final InterruptedException e) {
                    //ignored
                }
                return new Person(
                        x.getId(),
                        newFullName,
                        x.getDateOfBirth(),
                        x.isMarried(),
                        newHeightMeters,
                        x.getEyeColor());
            }
        };
        final var newExpectedPerson = new Person(
                expectedPerson.getId(),
                newFullName2,
                expectedPerson.getDateOfBirth(),
                expectedPerson.isMarried(),
                expectedPerson.getHeightMeters(),
                expectedPerson.getEyeColor());
        new Timer().schedule(new TimerTask() {
            @Override
            public void run() {
                insert(newExpectedPerson);
            }
        },100);
        final var updateResult = repository.update(expectedPerson.getId(), updater);
        assertTrue(updateResult.isPresent());
        assertFalse(updateResult.get());
        final var getResult = get(expectedPerson.getId());
        assertTrue(getResult.isPresent());
        assertNotEquals(expectedPerson, getResult.get());
        assertEquals(newExpectedPerson, getResult.get());
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

    private void insert(final Person person) {
        jedis.set("people:" + person.getId(), repository.convertTo(person));
    }

    private Optional<Person> get(final String id) {
        final var entity = jedis.get("people:" + id);
        return isNullOrEmptyOrBlank(entity) ? Optional.empty() : Optional.of(repository.convertFrom(entity));
    }

    private String randomString() {
        return UUID.randomUUID().toString();
    }
}
