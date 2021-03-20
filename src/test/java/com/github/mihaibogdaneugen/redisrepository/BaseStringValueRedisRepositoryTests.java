package com.github.mihaibogdaneugen.redisrepository;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.params.SetParams;

import java.io.UncheckedIOException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.github.mihaibogdaneugen.redisrepository.RedisRepository.isNullOrEmptyOrBlank;
import static org.junit.jupiter.api.Assertions.*;

final class BaseStringValueRedisRepositoryTests extends RedisTestContainer {

    static final Logger logger = LoggerFactory.getLogger(BaseStringValueRedisRepositoryTests.class);

    static Jedis jedis;
    static JedisPool jedisPool;
    static BaseStringValueRedisRepository<Person> repository;

    @BeforeAll
    static void beforeAll() {
        jedisPool = new JedisPool(
                REDIS_CONTAINER.getContainerIpAddress(),
                REDIS_CONTAINER.getMappedPort(REDIS_PORT));
        jedis = jedisPool.getResource();
        repository = new BaseStringValueRedisRepository<>(jedisPool, "people", jedisException -> logger.error(jedisException.getMessage(), jedisException)) {
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
        jedis.close();
        repository.close();
    }

    @BeforeEach
    void beforeEach() {
        jedis.flushAll();
    }

    @Test
    void testNewInstanceWithNullJedisPool() {
        final var nullJedisPoolError = assertThrows(IllegalArgumentException.class, () ->
                new BaseStringValueRedisRepository<Person>(null, randomString()) {
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
    void testNewInstanceWithInvalidCollectionKey() {
        final var nullCollectionKeyError = assertThrows(IllegalArgumentException.class, () ->
                new BaseStringValueRedisRepository<Person>(jedisPool, null) {
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
                new BaseStringValueRedisRepository<Person>(jedisPool, "") {
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
                new BaseStringValueRedisRepository<Person>(jedisPool, invalidCollectionKey) {
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
    void testNewInstanceWithNullJedisExceptionHandler() {
        final var nullJedisPoolError = assertThrows(IllegalArgumentException.class, () ->
                new BaseStringValueRedisRepository<Person>(jedisPool, randomString(), null) {
                    @Override
                    public String convertTo(final Person entity) {
                        return null;
                    }

                    @Override
                    public Person convertFrom(final String entityAsString) {
                        return null;
                    }
                });
        assertEquals("jedisExceptionInterceptor cannot be null!", nullJedisPoolError.getMessage());
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
                repository.get((Set<String>) null));
        assertEquals("ids cannot be null, nor empty!", nullIdError.getMessage());

        final var emptyIdError = assertThrows(IllegalArgumentException.class, () ->
                repository.get(Collections.emptySet()));
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
        final var actualResult = repository.get(Set.of(expectedPerson1.getId(), randomString(), expectedPerson3.getId()));
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
        final var actualResult = repository.get(Set.of(expectedPerson1.getId(), expectedPerson2.getId(), expectedPerson3.getId()));
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
        final var ids = expectedPeopleMap.keySet();
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
    void testSetIfExistsInvalidArgumentId() {
        final var person = Person.random();
        final var nullIdError = assertThrows(IllegalArgumentException.class, () ->
                repository.setIfItExists(null, person));
        assertEquals("id cannot be null, nor empty!", nullIdError.getMessage());

        final var emptyIdError = assertThrows(IllegalArgumentException.class, () ->
                repository.setIfItExists("", person));
        assertEquals("id cannot be null, nor empty!", emptyIdError.getMessage());
    }

    @Test
    void testSetIfExistsNullArgumentEntity() {
        final var person = Person.random();
        final var nullPersonError = assertThrows(IllegalArgumentException.class, () ->
                repository.setIfItExists(person.getId(), null));
        assertEquals("entity cannot be null!", nullPersonError.getMessage());
    }

    @Test
    void testSetIfExistsNonExisting() {
        final var expectedPerson = Person.random();
        repository.setIfItExists(expectedPerson.getId(), expectedPerson);
        final var actualPerson = get(expectedPerson.getId());
        assertTrue(actualPerson.isEmpty());
    }

    @Test
    void testSetIfExists() {
        final var oldPerson = Person.random();
        insert(oldPerson);
        final var expectedPerson = Person.random();
        expectedPerson.setId(oldPerson.getId());
        repository.setIfItExists(expectedPerson.getId(), expectedPerson);
        final var actualPerson = get(expectedPerson.getId());
        assertTrue(actualPerson.isPresent());
        assertEquals(expectedPerson, actualPerson.get());
    }

    @Test
    void testSetIfNotExistsInvalidArgumentId() {
        final var person = Person.random();
        final var nullIdError = assertThrows(IllegalArgumentException.class, () ->
                repository.setIfDoesNotExist(null, person));
        assertEquals("id cannot be null, nor empty!", nullIdError.getMessage());

        final var emptyIdError = assertThrows(IllegalArgumentException.class, () ->
                repository.setIfDoesNotExist("", person));
        assertEquals("id cannot be null, nor empty!", emptyIdError.getMessage());
    }

    @Test
    void testSetIfNotExistsNullArgumentEntity() {
        final var person = Person.random();
        final var nullPersonError = assertThrows(IllegalArgumentException.class, () ->
                repository.setIfDoesNotExist(person.getId(), null));
        assertEquals("entity cannot be null!", nullPersonError.getMessage());
    }

    @Test
    void testSetIfNotExistsReplace() {
        final var oldPerson = Person.random();
        insert(oldPerson);
        final var expectedPerson = Person.random();
        expectedPerson.setId(oldPerson.getId());
        repository.setIfDoesNotExist(expectedPerson.getId(), expectedPerson);
        final var actualPerson = get(expectedPerson.getId());
        assertTrue(actualPerson.isPresent());
        assertEquals(oldPerson, actualPerson.get());
    }

    @Test
    void testSetIfNotExists() {
        final var expectedPerson = Person.random();
        repository.setIfDoesNotExist(expectedPerson.getId(), expectedPerson);
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
    void testConditionalUpdateInvalidArgumentId() {
        final var nullIdError = assertThrows(IllegalArgumentException.class, () ->
                repository.update(null, person -> Person.random(), person -> true));
        assertEquals("id cannot be null, nor empty!", nullIdError.getMessage());

        final var emptyIdError = assertThrows(IllegalArgumentException.class, () ->
                repository.update(null, person -> null, person -> true));
        assertEquals("id cannot be null, nor empty!", emptyIdError.getMessage());
    }

    @Test
    void testConditionalUpdateNullUpdater() {
        final var nullIdError = assertThrows(IllegalArgumentException.class, () ->
                repository.update(randomString(), null, person -> true));
        assertEquals("updater cannot be null!", nullIdError.getMessage());

    }

    @Test
    void testConditionalUpdateNullConditioner() {
        final var nullIdError = assertThrows(IllegalArgumentException.class, () ->
                repository.update(randomString(), person -> Person.random(), null));
        assertEquals("conditioner cannot be null!", nullIdError.getMessage());
    }

    @Test
    void testConditionalUpdateNonExistingEntity() {
        final var person = Person.random();
        final var updateResult = repository.update(person.getId(), x -> new Person(
                x.getId(),
                randomString(),
                x.getDateOfBirth(),
                x.isMarried(),
                x.getHeightMeters(),
                x.getEyeColor()
        ), anything -> true);
        assertTrue(updateResult.isEmpty());
    }

    @Test
    void testConditionalUpdate() {
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
        final var updateResult = repository.update(expectedPerson.getId(), updater, person -> person.getHeightMeters() > 1.0f);
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
    void testConditionalUpdateFailedCondition() {
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
        final var updateResult = repository.update(expectedPerson.getId(), updater, person -> person.getHeightMeters() < 1.0f);
        assertTrue(updateResult.isPresent());
        assertTrue(updateResult.get());
        final var getResult = get(expectedPerson.getId());
        assertTrue(getResult.isPresent());
        assertEquals(expectedPerson, getResult.get());
    }

    @Test
    void testConditionalUpdateTransactionalBehaviour() {
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
        final var updateResult = repository.update(expectedPerson.getId(), updater, person -> {
            try {
                Thread.sleep(100);
            } catch (final InterruptedException e) {
                //ignored
            }
            return person.getHeightMeters() > 1.0f;
        });
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
    void testConditionalDeleteInvalidArgument() {
        final var nullIdError = assertThrows(IllegalArgumentException.class, () ->
                repository.delete(null, person -> true));
        assertEquals("id cannot be null, nor empty!", nullIdError.getMessage());

        final var emptyIdError = assertThrows(IllegalArgumentException.class, () ->
                repository.delete("", person -> true));
        assertEquals("id cannot be null, nor empty!", emptyIdError.getMessage());
    }

    @Test
    void testConditionalDeleteNullConditioner() {
        final var nullIdError = assertThrows(IllegalArgumentException.class, () ->
                repository.delete(randomString(), null));
        assertEquals("conditioner cannot be null!", nullIdError.getMessage());
    }

    @Test
    void testConditionalDeleteNonExistentEntity() {
        final var expectedPerson = Person.random();
        final var result = repository.delete(expectedPerson.getId(), person -> person.getHeightMeters() > 1.0f);
        assertTrue(result.isEmpty());
    }

    @Test
    void testConditionalDelete() {
        final var expectedPerson = Person.random();
        insert(expectedPerson);
        final var deleteResult = repository.delete(expectedPerson.getId(), person -> person.getHeightMeters() > 1.0f);
        assertTrue(deleteResult.isPresent());
        assertTrue(deleteResult.get());
        final var getResult = get(expectedPerson.getId());
        assertTrue(getResult.isEmpty());
    }

    @Test
    void testConditionalDeleteFailedCondition() {
        final var expectedPerson = Person.random();
        insert(expectedPerson);
        final var deleteResult = repository.delete(expectedPerson.getId(), person -> person.getHeightMeters() > 2.0f);
        assertTrue(deleteResult.isPresent());
        assertTrue(deleteResult.get());
        final var getResult = get(expectedPerson.getId());
        assertTrue(getResult.isPresent());
        assertEquals(expectedPerson, getResult.get());
    }

    @Test
    void testConditionalDeleteTransactionalBehaviour() {
        final var expectedPerson = Person.random();
        insert(expectedPerson);
        final var newExpectedPerson = new Person(
                expectedPerson.getId(),
                randomString(),
                expectedPerson.getDateOfBirth(),
                expectedPerson.isMarried(),
                10 * expectedPerson.getHeightMeters(),
                expectedPerson.getEyeColor());
        new Timer().schedule(new TimerTask() {
            @Override
            public void run() {
                insert(newExpectedPerson);
            }
        },100);
        final var deleteResult = repository.delete(expectedPerson.getId(), person -> {
            try {
                Thread.sleep(100);
            } catch (final InterruptedException e) {
                //ignored
            }
            return person.getHeightMeters() > 1.0f;
        });
        assertTrue(deleteResult.isPresent());
        assertFalse(deleteResult.get());
        final var getResult = get(expectedPerson.getId());
        assertTrue(getResult.isPresent());
        assertNotEquals(expectedPerson, getResult.get());
        assertEquals(newExpectedPerson, getResult.get());
    }

    @Test
    void testDeleteMultipleInvalidArgument() {
        final var nullIdError = assertThrows(IllegalArgumentException.class, () ->
                repository.delete((Set<String>) null));
        assertEquals("ids cannot be null, nor empty!", nullIdError.getMessage());

        final var emptyIdError = assertThrows(IllegalArgumentException.class, () ->
                repository.delete(Collections.emptySet()));
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
        repository.delete(Set.of(expectedPerson1.getId(), randomString(), expectedPerson3.getId()));
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
        repository.delete(Set.of(expectedPerson1.getId(), expectedPerson2.getId(), expectedPerson3.getId()));
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
        repository.delete(expectedPeopleMap.keySet());
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
    void testSetExpirationAfterInvalidArgumentId() {
        final var nullIdError = assertThrows(IllegalArgumentException.class, () ->
                repository.setExpirationAfter(null, 1000));
        assertEquals("id cannot be null, nor empty!", nullIdError.getMessage());

        final var emptyIdError = assertThrows(IllegalArgumentException.class, () ->
                repository.setExpirationAfter("", 1000));
        assertEquals("id cannot be null, nor empty!", emptyIdError.getMessage());
    }

    @Test
    void testSetExpirationAfterInvalidArgumentMilliseconds() {
        final var invalidMillisecondsError = assertThrows(IllegalArgumentException.class, () ->
                repository.setExpirationAfter(randomString(), -1));
        assertEquals("milliseconds cannot have a negative value!", invalidMillisecondsError.getMessage());
    }

    @Test
    void testSetExpirationAfter() throws InterruptedException {
        final var person1 = Person.random();
        insert(person1);
        final var person2 = Person.random();
        insert(person2);
        repository.setExpirationAfter(person2.getId(), 500);
        Thread.sleep(1000);
        final var result1 = get(person1.getId());
        assertTrue(result1.isPresent());
        assertEquals(person1, result1.get());
        final var result2 = get(person2.getId());
        assertTrue(result2.isEmpty());
        repository.setExpirationAfter(randomString(), 500); //does not fail for non existing entities
    }

    @Test
    void testSetExpirationAtInvalidArgumentId() {
        final var nullIdError = assertThrows(IllegalArgumentException.class, () ->
                repository.setExpirationAt(null, 1000));
        assertEquals("id cannot be null, nor empty!", nullIdError.getMessage());

        final var emptyIdError = assertThrows(IllegalArgumentException.class, () ->
                repository.setExpirationAt("", 1000));
        assertEquals("id cannot be null, nor empty!", emptyIdError.getMessage());
    }

    @Test
    void testSetExpirationAtInvalidArgumentMilliseconds() {
        final var invalidMillisecondsError = assertThrows(IllegalArgumentException.class, () ->
                repository.setExpirationAt(randomString(), -1));
        assertEquals("millisecondsTimestamp cannot have a negative value!", invalidMillisecondsError.getMessage());
    }

    @Test
    void testSetExpirationAt() throws InterruptedException {
        final var person1 = Person.random();
        insert(person1);
        final var person2 = Person.random();
        insert(person2);
        repository.setExpirationAt(person2.getId(), System.currentTimeMillis() + 500);
        Thread.sleep(500);
        final var result1 = get(person1.getId());
        assertTrue(result1.isPresent());
        assertEquals(person1, result1.get());
        final var result2 = get(person2.getId());
        assertTrue(result2.isEmpty());
        repository.setExpirationAt(randomString(), System.currentTimeMillis()); //does not fail for non existing entities
    }

    @Test
    void testGetTimeToLiveLeftInvalidArgumentId() {
        final var nullIdError = assertThrows(IllegalArgumentException.class, () ->
                repository.getTimeToLiveLeft(null));
        assertEquals("id cannot be null, nor empty!", nullIdError.getMessage());

        final var emptyIdError = assertThrows(IllegalArgumentException.class, () ->
                repository.getTimeToLiveLeft(""));
        assertEquals("id cannot be null, nor empty!", emptyIdError.getMessage());
    }

    @Test
    void testGetTimeToLiveLeft() {
        final var person = Person.random();
        jedis.set("people:" + person.getId(), repository.convertTo(person), SetParams.setParams().px(1000));
        final var ttl = repository.getTimeToLiveLeft(person.getId());
        assertTrue(0 <= ttl && ttl <= 1000);
        repository.getTimeToLiveLeft(randomString()); //does not fail for non existing entities
    }

    @Test
    void testGetAllKeys() {
        final var noKeys = repository.getAllKeys();
        assertTrue(noKeys.isEmpty());
        final var expectedPeopleMap = IntStream.range(0, 50)
                .mapToObj(i -> Person.random())
                .collect(Collectors.toMap(Person::getId, person -> person));
        expectedPeopleMap.values().forEach(this::insert);
        final var allKeys = repository.getAllKeys();
        assertEquals(50, allKeys.size());
        expectedPeopleMap.keySet().stream()
                .map(id -> "people:" + id)
                .forEach(key -> assertTrue(allKeys.contains(key)));
        jedis.del(allKeys.toArray(String[]::new));
        final var noMoreKeys = repository.getAllKeys();
        assertTrue(noMoreKeys.isEmpty());
    }

    @Test
    void testUpdateIfItIs() {
        final var oldPerson = Person.random();
        insert(oldPerson);
        final var newPerson = new Person(
                oldPerson.getId(),
                randomString(),
                oldPerson.getDateOfBirth(),
                oldPerson.isMarried(),
                oldPerson.getHeightMeters(),
                oldPerson.getEyeColor()
        );
        repository.updateIfItIs(oldPerson.getId(), oldPerson, newPerson);
        final var result = get(oldPerson.getId());
        assertTrue(result.isPresent());
        assertNotEquals(oldPerson, result.get());
        assertEquals(newPerson, result.get());
    }

    @Test
    void testUpdateIfItIsNot() {
        final var oldPerson = Person.random();
        insert(oldPerson);
        final var wrongOldPerson = new Person(
                oldPerson.getId(),
                randomString(),
                oldPerson.getDateOfBirth(),
                oldPerson.isMarried(),
                oldPerson.getHeightMeters(),
                oldPerson.getEyeColor()
        );
        final var newPerson = new Person(
                oldPerson.getId(),
                randomString(),
                oldPerson.getDateOfBirth(),
                oldPerson.isMarried(),
                oldPerson.getHeightMeters(),
                oldPerson.getEyeColor()
        );
        repository.updateIfItIsNot(oldPerson.getId(), wrongOldPerson, newPerson);
        final var result = get(oldPerson.getId());
        assertTrue(result.isPresent());
        assertNotEquals(oldPerson, result.get());
        assertNotEquals(wrongOldPerson, result.get());
        assertEquals(newPerson, result.get());
    }

    @Test
    void testDeleteIfItIs() {
        final var oldPerson = Person.random();
        insert(oldPerson);
        repository.deleteIfItIs(oldPerson.getId(), oldPerson);
        final var result = get(oldPerson.getId());
        assertTrue(result.isEmpty());
    }

    @Test
    void testDeleteIfItIsNot() {
        final var oldPerson = Person.random();
        insert(oldPerson);
        final var wrongOldPerson = new Person(
                oldPerson.getId(),
                randomString(),
                oldPerson.getDateOfBirth(),
                oldPerson.isMarried(),
                oldPerson.getHeightMeters(),
                oldPerson.getEyeColor()
        );
        repository.deleteIfItIsNot(oldPerson.getId(), wrongOldPerson);
        final var result = get(oldPerson.getId());
        assertTrue(result.isEmpty());
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
