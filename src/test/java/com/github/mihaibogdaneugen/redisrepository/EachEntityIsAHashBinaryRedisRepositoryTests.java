package com.github.mihaibogdaneugen.redisrepository;

import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.util.SafeEncoder;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

final class EachEntityIsAHashBinaryRedisRepositoryTests extends RedisTestContainer {

    static final Logger logger = LoggerFactory.getLogger(EachEntityIsAHashBinaryRedisRepositoryTests.class);
    static final byte[] binaryId = SafeEncoder.encode("id");
    static final byte[] binaryFullName = SafeEncoder.encode("fullName");
    static final byte[] binaryDateOfBirth = SafeEncoder.encode("dateOfBirth");
    static final byte[] binaryIsMarried = SafeEncoder.encode("isMarried");
    static final byte[] binaryHeightMeters = SafeEncoder.encode("heightMeters");
    static final byte[] binaryEyeColor = SafeEncoder.encode("eyeColor");
    static final Function<Person, Map<byte[], byte[]>> serializer = person -> {
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
    };
    static final Function<Map<byte[], byte[]>, Person> deserializer = fields -> {
        final var person = new Person();
        Optional.ofNullable(fields.get(binaryId))
                .map(SafeEncoder::encode)
                .ifPresent(person::setId);
        Optional.ofNullable(fields.get(binaryFullName))
                .map(SafeEncoder::encode)
                .ifPresent(person::setFullName);
        Optional.ofNullable(fields.get(binaryDateOfBirth))
                .map(SafeEncoder::encode)
                .map(value -> LocalDate.parse(value, DateTimeFormatter.ISO_LOCAL_DATE))
                .ifPresent(person::setDateOfBirth);
        Optional.ofNullable(fields.get(binaryIsMarried))
                .map(SafeEncoder::encode)
                .map(Boolean::parseBoolean)
                .ifPresent(person::setMarried);
        Optional.ofNullable(fields.get(binaryHeightMeters))
                .map(SafeEncoder::encode)
                .map(Float::parseFloat)
                .ifPresent(person::setHeightMeters);
        Optional.ofNullable(fields.get(binaryEyeColor))
                .map(SafeEncoder::encode)
                .map(Person.EyeColor::valueOf)
                .ifPresent(person::setEyeColor);
        return person;
    };

    static Jedis jedis;
    static JedisPool jedisPool;
    static BaseRedisRepository<Person> repository;

    @BeforeAll
    static void beforeAll() {
        jedisPool = new JedisPool(
                REDIS_CONTAINER.getContainerIpAddress(),
                REDIS_CONTAINER.getMappedPort(REDIS_PORT));
        jedis = jedisPool.getResource();
        final var configuration = RedisRepositoryConfiguration.<Person>builder()
                .jedisPool(jedisPool)
                .jedisExceptionInterceptor(jedisException -> logger.error(jedisException.getMessage(), jedisException))
                .collectionKey("people")
                .keySeparator(":")
                .strategyBinaryEachEntityIsAHash(RedisRepositoryStrategy.BinaryEachEntityIsAHash.<Person>builder()
                        .serializer(serializer)
                        .deserializer(deserializer)
                        .build())
                .build();
        repository = new BaseRedisRepository<>(configuration) { };
    }

    @AfterAll
    static void afterAll() {
        jedis.close();
        jedisPool.close();
        repository.close();
    }

    @BeforeEach
    void beforeEach() {
        jedis.flushAll();
    }


    @Nested
    final class TestsForGet {
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
            jedis.set(SafeEncoder.encode("people:" + expectedPerson.getId()), new byte[0]);
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
    }

    @Nested
    final class TestsForGetMultiple {
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
            expectedPeopleMap.values().forEach(EachEntityIsAHashBinaryRedisRepositoryTests::insert);
            final var ids = expectedPeopleMap.keySet();
            final var actualResult = repository.get(ids);
            assertEquals(50, actualResult.size());
            final var actualPeopleMap = actualResult.stream().collect(Collectors.toMap(Person::getId, x -> x));
            expectedPeopleMap.forEach((key, value) -> assertEquals(value, actualPeopleMap.get(key)));
        }
    }

    @Nested
    final class TestsForGetAll {
        @Test
        void testGetAll() {
            final var expectedPeopleMap = IntStream.range(0, 50)
                    .mapToObj(i -> Person.random())
                    .collect(Collectors.toMap(Person::getId, person -> person));
            expectedPeopleMap.values().forEach(EachEntityIsAHashBinaryRedisRepositoryTests::insert);
            final var actualResult = repository.getAll();
            assertEquals(50, actualResult.size());
            final var actualPeopleMap = actualResult.stream().collect(Collectors.toMap(Person::getId, x -> x));
            expectedPeopleMap.forEach((key, value) -> assertEquals(value, actualPeopleMap.get(key)));
        }
    }

    @Nested
    final class TestsForExists {
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
    }

    @Nested
    final class TestsForSet {
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
    }

    @Nested
    final class TestsForSetIfItDoesExist {
        @Test
        void testSetIfItDoesExistInvalidArgumentId() {
            final var person = Person.random();
            final var nullIdError = assertThrows(IllegalArgumentException.class, () ->
                    repository.setIfItDoesExist(null, person));
            assertEquals("id cannot be null, nor empty!", nullIdError.getMessage());

            final var emptyIdError = assertThrows(IllegalArgumentException.class, () ->
                    repository.setIfItDoesExist("", person));
            assertEquals("id cannot be null, nor empty!", emptyIdError.getMessage());
        }

        @Test
        void testSetIfItDoesExistNullArgumentEntity() {
            final var person = Person.random();
            final var nullPersonError = assertThrows(IllegalArgumentException.class, () ->
                    repository.setIfItDoesExist(person.getId(), null));
            assertEquals("entity cannot be null!", nullPersonError.getMessage());
        }

        @Test
        void testSetIfItDoesExistNonExisting() {
            final var expectedPerson = Person.random();
            repository.setIfItDoesExist(expectedPerson.getId(), expectedPerson);
            final var actualPerson = get(expectedPerson.getId());
            assertTrue(actualPerson.isEmpty());
        }

        @Test
        void testSetIfItDoesExist() {
            final var oldPerson = Person.random();
            insert(oldPerson);
            final var expectedPerson = Person.random();
            expectedPerson.setId(oldPerson.getId());
            repository.setIfItDoesExist(expectedPerson.getId(), expectedPerson);
            final var actualPerson = get(expectedPerson.getId());
            assertTrue(actualPerson.isPresent());
            assertEquals(expectedPerson, actualPerson.get());
        }
    }

    @Nested
    final class TestsForSetIfItDoesNotExist {
        @Test
        void testSetIfItDoesNotExistInvalidArgumentId() {
            final var person = Person.random();
            final var nullIdError = assertThrows(IllegalArgumentException.class, () ->
                    repository.setIfItDoesNotExist(null, person));
            assertEquals("id cannot be null, nor empty!", nullIdError.getMessage());

            final var emptyIdError = assertThrows(IllegalArgumentException.class, () ->
                    repository.setIfItDoesNotExist("", person));
            assertEquals("id cannot be null, nor empty!", emptyIdError.getMessage());
        }

        @Test
        void testSetIfItDoesNotExistNullArgumentEntity() {
            final var person = Person.random();
            final var nullPersonError = assertThrows(IllegalArgumentException.class, () ->
                    repository.setIfItDoesNotExist(person.getId(), null));
            assertEquals("entity cannot be null!", nullPersonError.getMessage());
        }

        @Test
        void testSetIfItDoesNotExistReplace() {
            final var oldPerson = Person.random();
            insert(oldPerson);
            final var expectedPerson = Person.random();
            expectedPerson.setId(oldPerson.getId());
            repository.setIfItDoesNotExist(expectedPerson.getId(), expectedPerson);
            final var actualPerson = get(expectedPerson.getId());
            assertTrue(actualPerson.isPresent());
            assertEquals(oldPerson, actualPerson.get());
        }

        @Test
        void testSetIfItDoesNotExist() {
            final var expectedPerson = Person.random();
            repository.setIfItDoesNotExist(expectedPerson.getId(), expectedPerson);
            final var actualPerson = get(expectedPerson.getId());
            assertTrue(actualPerson.isPresent());
            assertEquals(expectedPerson, actualPerson.get());
        }
    }

    @Nested
    final class TestsForUpdate {
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
    }

    @Nested
    final class TestsForUpdateConditional {
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
        void testConditionalUpdateNullCondition() {
            final var nullIdError = assertThrows(IllegalArgumentException.class, () ->
                    repository.update(randomString(), person -> Person.random(), null));
            assertEquals("condition cannot be null!", nullIdError.getMessage());
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
    }

    @Nested
    final class TestsForDeleteOne {
        @Test
        void testDeleteInvalidArgument() {
            final var nullIdError = assertThrows(IllegalArgumentException.class, () ->
                    repository.delete((String) null));
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
    }

    @Nested
    final class TestsForDeleteConditional {
        @Test
        void testConditionalDeleteInvalidArgumentId() {
            final var nullIdError = assertThrows(IllegalArgumentException.class, () ->
                    repository.delete(null, person -> true));
            assertEquals("id cannot be null, nor empty!", nullIdError.getMessage());

            final var emptyIdError = assertThrows(IllegalArgumentException.class, () ->
                    repository.delete("", person -> true));
            assertEquals("id cannot be null, nor empty!", emptyIdError.getMessage());
        }

        @Test
        void testConditionalDeleteNullCondition() {
            final var nullIdError = assertThrows(IllegalArgumentException.class, () ->
                    repository.delete(randomString(), null));
            assertEquals("condition cannot be null!", nullIdError.getMessage());
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
            }, 100);
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
    }

    @Nested
    final class TestsForDeleteMultiple {
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
            expectedPeopleMap.values().forEach(EachEntityIsAHashBinaryRedisRepositoryTests::insert);
            repository.delete(expectedPeopleMap.keySet());
            expectedPeopleMap.keySet().forEach(key -> {
                final var result = get(key);
                assertTrue(result.isEmpty());
            });
        }
    }

    @Nested
    final class TestsForDeleteAll {
        @Test
        void testDeleteAll() {
            final var expectedPeopleMap = IntStream.range(0, 50)
                    .mapToObj(i -> Person.random())
                    .collect(Collectors.toMap(Person::getId, person -> person));
            expectedPeopleMap.values().forEach(EachEntityIsAHashBinaryRedisRepositoryTests::insert);
            repository.deleteAll();
            expectedPeopleMap.keySet().forEach(key -> {
                final var result = get(key);
                assertTrue(result.isEmpty());
            });
        }
    }

    @Nested
    final class TestsForGetAllIds {
        @Test
        void testGetAllIds() {
            final var noKeys = repository.getAllIds();
            assertTrue(noKeys.isEmpty());
            final var expectedPeopleMap = IntStream.range(0, 50)
                    .mapToObj(i -> Person.random())
                    .collect(Collectors.toMap(Person::getId, person -> person));
            expectedPeopleMap.values().forEach(EachEntityIsAHashBinaryRedisRepositoryTests::insert);
            final var allIds = repository.getAllIds();
            assertEquals(50, allIds.size());
            expectedPeopleMap.keySet()
                    .forEach(key -> assertTrue(allIds.contains(key)));
            jedis.del(allIds.stream().map(id -> SafeEncoder.encode("people:" + id)).toArray(byte[][]::new));
            final var noMoreKeys = repository.getAllIds();
            assertTrue(noMoreKeys.isEmpty());
        }
    }

    @Nested
    final class TestsForLuaScripts {
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
            final var unsupportedOperationException = assertThrows(UnsupportedOperationException.class, () ->
                    repository.updateIfItIs(oldPerson.getId(), oldPerson, newPerson));
            assertEquals("Unsupported RedisRepositoryStrategy!", unsupportedOperationException.getMessage());
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
            final var unsupportedOperationException = assertThrows(UnsupportedOperationException.class, () ->
                    repository.updateIfItIsNot(oldPerson.getId(), wrongOldPerson, newPerson));
            assertEquals("Unsupported RedisRepositoryStrategy!", unsupportedOperationException.getMessage());
        }

        @Test
        void testDeleteIfItIs() {
            final var oldPerson = Person.random();
            insert(oldPerson);
            final var unsupportedOperationException = assertThrows(UnsupportedOperationException.class, () ->
                    repository.deleteIfItIs(oldPerson.getId(), oldPerson));
            assertEquals("Unsupported RedisRepositoryStrategy!", unsupportedOperationException.getMessage());
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
            final var unsupportedOperationException = assertThrows(UnsupportedOperationException.class, () ->
                    repository.deleteIfItIsNot(oldPerson.getId(), wrongOldPerson));
            assertEquals("Unsupported RedisRepositoryStrategy!", unsupportedOperationException.getMessage());
        }
    }

    private static void insert(final Person person) {
        jedis.hset(SafeEncoder.encode("people:" + person.getId()), serializer.apply(person));
    }

    private static Optional<Person> get(final String id) {
        final var entity = jedis.hgetAll(SafeEncoder.encode("people:" + id));
        return entity == null || entity.isEmpty() ? Optional.empty() : Optional.of(deserializer.apply(entity));
    }

    private static String randomString() {
        return UUID.randomUUID().toString();
    }
}
