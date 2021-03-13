package com.github.mihaibogdaneugen.redisrepository;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.github.mihaibogdaneugen.redisrepository.RedisRepository.isNullOrEmpty;
import static org.junit.jupiter.api.Assertions.*;

final class BaseStringHashRedisRepositoryTests extends RedisTestContainer {

    static Jedis jedis;
    static JedisPool jedisPool;
    static BaseStringHashRedisRepository<Person> repository;

    @BeforeAll
    static void beforeAll() {
        jedisPool = new JedisPool(
                REDIS_CONTAINER.getContainerIpAddress(),
                REDIS_CONTAINER.getMappedPort(REDIS_PORT));
        jedis = jedisPool.getResource();
        repository = new BaseStringHashRedisRepository<>(jedis, "people") {

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
                new BaseStringHashRedisRepository<Person>((Jedis) null, randomString()) {
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
                new BaseStringHashRedisRepository<Person>((JedisPool) null, randomString()) {
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
                new BaseStringHashRedisRepository<Person>(jedis, null) {
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
                new BaseStringHashRedisRepository<Person>(jedis, "") {
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
                new BaseStringHashRedisRepository<Person>(jedis, invalidCollectionKey) {
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
                new BaseStringHashRedisRepository<Person>(jedisPool, null) {
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
                new BaseStringHashRedisRepository<Person>(jedisPool, "") {
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
                new BaseStringHashRedisRepository<Person>(jedisPool, invalidCollectionKey) {
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
        Thread.sleep(500);
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
        insert(person);
        jedis.pexpire("people:" + person.getId(), 500);
        final var ttl = repository.getTimeToLiveLeft(person.getId());
        assertTrue(0 < ttl && ttl < 500);
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
