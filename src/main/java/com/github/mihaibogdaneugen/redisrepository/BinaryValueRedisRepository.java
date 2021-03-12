package com.github.mihaibogdaneugen.redisrepository;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.params.SetParams;
import redis.clients.jedis.util.SafeEncoder;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A RedisRepository for a specified entity type, where all entities are serialized as binary values <br/>
 * and stored as key-value pairs (simple Redis values). <br/>
 * Design details:<br/>
 * - every entity has a String identifier, but this is not enforced as part of the type itself.<br/>
 * - every entity is part of a collection that groups all entities with the same type<br/>
 * - the actual key of a given entity is composed as the `${collection_key}:${entity_id}`<br/>
 * Note: This repository optimises for simple and transactional `insert`, `update`, `delete_one`, `get_one`,<br/>
 * `get_some`, `delete_some` operations, but it's not recommended for `get_all` or `delete_all` operations. <br/>
 * These last two operations are not implemented in a transactional manner and they're highly inefficient <br/>
 * for large collections.
 * @param <T> The type of the entity
 */
public abstract class BinaryValueRedisRepository<T> extends BaseRedisRepository<T> {

    private final String keyPrefix;
    private final byte[] allKeysPattern;

    /**
     * Builds a BinaryValueRedisRepository, based around a Jedis object, for a specific collection.<br/>
     * The provided Jedis object will be closed should `.close()` be called.
     * @param jedis The Jedis object
     * @param collectionKey The name (key) of the collection
     */
    public BinaryValueRedisRepository(final Jedis jedis, final String collectionKey) {
        super(jedis);
        throwIfNullOrEmptyOrBlank(collectionKey, "collectionKey");
        if (collectionKey.contains(DEFAULT_KEY_SEPARATOR)) {
            throw new IllegalArgumentException("Collection key `" + collectionKey + "` cannot contain `" + DEFAULT_KEY_SEPARATOR + "`");
        }
        keyPrefix = collectionKey + DEFAULT_KEY_SEPARATOR;
        allKeysPattern = SafeEncoder.encode(collectionKey + DEFAULT_KEY_SEPARATOR + "*");
    }

    /**
     * Builds a BinaryValueRedisRepository, based around a jedisPool object, for a specific collection.<br/>
     * A Jedis object will be retrieved from the JedisPool by calling `.getResource()` and it will<br/>
     * be closed should `.close()` be called.
     * @param jedisPool The JedisPool object
     * @param collectionKey The name (key) of the collection
     */
    public BinaryValueRedisRepository(final JedisPool jedisPool, final String collectionKey) {
        super(jedisPool);
        throwIfNullOrEmptyOrBlank(collectionKey, "collectionKey");
        if (collectionKey.contains(DEFAULT_KEY_SEPARATOR)) {
            throw new IllegalArgumentException("Collection key `" + collectionKey + "` cannot contain `" + DEFAULT_KEY_SEPARATOR + "`");
        }
        keyPrefix = collectionKey + DEFAULT_KEY_SEPARATOR;
        allKeysPattern = SafeEncoder.encode(collectionKey + DEFAULT_KEY_SEPARATOR + "*");
    }

    /**
     * Converts the given entity to a binary value.
     * @param entity The entity to be converted
     * @return A binary value object
     */
    public abstract byte[] convertTo(final T entity);

    /**
     * Converts back the given binary value to an entity.
     * @param entityAsBinary The binary value representation of the entity
     * @return An entity object
     */
    public abstract T convertFrom(final byte[] entityAsBinary);

    /**
     * Retrieves the entity with the given identifier.<br/>
     * Note: This method calls the GET Redis command.
     * @see <a href="https://redis.io/commands/GET">GET</a>
     * @param id The String identifier of the entity
     * @return Optional object, empty if no such entity is found, or the object otherwise
     */
    @Override
    public final Optional<T> get(final String id) {
        throwIfNullOrEmptyOrBlank(id, "id");
        final var key = getKey(id);
        final var entity = jedis.get(key);
        return isNullOrEmpty(entity)
                ? Optional.empty()
                : Optional.of(convertFrom(entity));
    }

    /**
     * Retrieves the entities with the given identifiers.<br/>
     * Note: This method calls the MGET Redis command.
     * @see <a href="https://redis.io/commands/MGET">MGET</a>
     * @param ids The array of Strings identifiers of entities
     * @return A list of entities
     */
    @Override
    public final List<T> get(final String... ids) {
        throwIfNullOrEmpty(ids);
        final var keys = getKeys(ids);
        return getByKeys(keys);
    }

    /**
     * Retrieves all entities from the current collection.<br/>
     * This method first retrieves all keys first and then all entities, <br/>
     * but it does not provide any transactional behaviour. <br/>
     * Warning: Do not use this method with large databases due to poor performance of KEYS Redis command.<br/>
     * Note: This method calls the KEYS and MGET Redis commands.
     * @see <a href="https://redis.io/commands/KEYS">KEYS</a>
     * @see <a href="https://redis.io/commands/MGET">MGET</a>
     * @return A list of entities
     */
    @Override
    public final List<T> getAll() {
        final var keys = getAllKeys();
        return getByKeys(keys);
    }

    /**
     * Checks if the entity with the specified identifier exists in the repository or not.<br/>
     * Note: This method calls the EXISTS Redis command.
     * @see <a href="https://redis.io/commands/EXISTS">EXISTS</a>
     * @param id The String identifier of the entity
     * @return A Boolean object, true if it exists, false otherwise
     */
    @Override
    public final Boolean exists(final String id) {
        throwIfNullOrEmptyOrBlank(id, "id");
        final var key = getKey(id);
        return jedis.exists(key);
    }

    /**
     * Replaces (or inserts) the given entity with the specified identifier.<br/>
     * Note: This method calls the SET Redis command.
     * @see <a href="https://redis.io/commands/SET">SET</a>
     * @param id The String identifier of the entity
     * @param entity The entity to be set
     */
    @Override
    public final void set(final String id, final T entity) {
        throwIfNullOrEmptyOrBlank(id, "id");
        throwIfNull(entity, "entity");
        final var key = getKey(id);
        jedis.set(key, convertTo(entity));
    }

    /**
     * Inserts the given entity with the specified identifier, only if it does not exist.<br/>
     * Note: This method calls the SETNX Redis command.
     * @see <a href="https://redis.io/commands/SETNX">SETNX</a>
     * @param id The String identifier of the entity
     * @param entity The entity to be set
     */
    @Override
    public final void setIfNotExist(final String id, final T entity) {
        throwIfNullOrEmptyOrBlank(id, "id");
        throwIfNull(entity, "entity");
        final var key = getKey(id);
        jedis.setnx(key, convertTo(entity));
    }

    /**
     * Updates the entity with the specified identifier by calling the `updater` function.<br/>
     * This method provides a transactional behaviour for updating the entity.<br/>
     * Note: This method calls the WATCH, GET, UNWATCH, MULTI, SET and EXEC Redis commands.
     * @see <a href="https://redis.io/commands/WATCH">WATCH</a>
     * @see <a href="https://redis.io/commands/GET">GET</a>
     * @see <a href="https://redis.io/commands/UNWATCH">UNWATCH</a>
     * @see <a href="https://redis.io/commands/MULTI">MULTI</a>
     * @see <a href="https://redis.io/commands/SET">SET</a>
     * @see <a href="https://redis.io/commands/EXEC">EXEC</a>
     * @param id The String identifier of the entity
     * @param updater A function that updates the entity
     * @return Optional object, empty if no such entity exists, or boolean value indicating the status of the transaction
     */
    @Override
    public final Optional<Boolean> update(final String id, final Function<T, T> updater) {
        throwIfNullOrEmptyOrBlank(id, "id");
        throwIfNull(updater, "updater");
        final var key = getKey(id);
        jedis.watch(key);
        final var value = jedis.get(key);
        if (isNullOrEmpty(value)) {
            jedis.unwatch();
            return Optional.empty();
        }
        final var entity = convertFrom(value);
        final var newEntity = updater.apply(entity);
        final var newValue = convertTo(newEntity);
        final List<Object> results;
        try (final var transaction = jedis.multi()) {
            transaction.set(key, newValue);
            results = transaction.exec();
        }
        return Optional.of(isNotNullNorEmpty(results));
    }

    /**
     * Removes the entity with the given identifier.<br/>
     * Note: This method calls the DEL Redis command.
     * @see <a href="https://redis.io/commands/DEL">DEL</a>
     * @param id The String identifier of the entity
     */
    @Override
    public final void delete(final String id) {
        throwIfNullOrEmptyOrBlank(id, "id");
        final var key = getKey(id);
        jedis.del(key);
    }

    /**
     * Removes all entities with the given identifiers.<br/>
     * Note: This method calls the DEL Redis command.
     * @see <a href="https://redis.io/commands/DEL">DEL</a>
     * @param ids The array of Strings identifiers of entities
     */
    @Override
    public final void delete(final String... ids) {
        throwIfNullOrEmpty(ids);
        final var keys = getKeys(ids);
        jedis.del(keys);
    }

    /**
     * Removes all entities from the current collection.<br/>
     * This method first retrieves all keys first and then deletes all entities, <br/>
     * but it does not provide any transactional behaviour. <br/>
     * Warning: Do not use this method with large databases due to poor performance of KEYS Redis command.<br/>
     * Note: This method calls the KEYS and DEL Redis commands.
     * @see <a href="https://redis.io/commands/KEYS">KEYS</a>
     * @see <a href="https://redis.io/commands/DEL">DEL</a>
     */
    @Override
    public final void deleteAll() {
        final var keys = getAllKeys();
        jedis.del(keys);
    }

    /**
     * Replace (or insert) the given entity with the specified identifier.<br/>
     * Note: This method calls the SET Redis command.
     * @see <a href="https://redis.io/commands/SET">SET</a>
     * @param id The String identifier of the entity
     * @param entity The entity to be set
     * @param setParams The SetParams object providing more flags
     */
    public final void set(final String id, final T entity, final SetParams setParams) {
        throwIfNullOrEmptyOrBlank(id, "id");
        throwIfNull(entity, "entity");
        throwIfNull(setParams, "setParams");
        final var key = getKey(id);
        jedis.set(key, convertTo(entity), setParams);
    }

    /**
     * Sets the expiration after the given number of milliseconds for the entity with the given identifier.<br/>
     * Note: This method calls the PEXPIRE Redis command.
     * @see <a href="https://redis.io/commands/PEXPIRE">PEXPIRE</a>
     * @param id The String identifier of the entity
     * @param milliseconds The number of milliseconds after which the entity will expire
     */
    public final void setExpirationAfter(final String id, final long milliseconds) {
        throwIfNullOrEmptyOrBlank(id, "id");
        throwIfNegative(milliseconds, "milliseconds");
        final var key = getKey(id);
        jedis.pexpire(key, milliseconds);
    }

    /**
     * Sets the expiration at the given timestamp (Unix time) for the entity with the given identifier.<br/>
     * Note: This method calls the PEXPIREAT Redis command.
     * @see <a href="https://redis.io/commands/PEXPIREAT">PEXPIREAT</a>
     * @param id The String identifier of the entity
     * @param millisecondsTimestamp The timestamp (Unix time) when the entity will expire
     */
    public final void setExpirationAt(final String id, final long millisecondsTimestamp) {
        throwIfNullOrEmptyOrBlank(id, "id");
        throwIfNegative(millisecondsTimestamp, "millisecondsTimestamp");
        final var key = getKey(id);
        jedis.pexpireAt(key, millisecondsTimestamp);
    }

    /**
     * Returns the time to live left in milliseconds till the entity will expire.<br/>
     * Note: This method calls the PTTL Redis command.
     * @see <a href="https://redis.io/commands/PTTL">PTTL</a>
     * @param id The String identifier of the entity
     * @return No. of milliseconds
     */
    public final Long getTimeToLiveLeft(final String id) {
        throwIfNullOrEmptyOrBlank(id, "id");
        final var key = getKey(id);
        return jedis.pttl(key);
    }

    private byte[] getKey(final String keySuffix) {
        return SafeEncoder.encode(keyPrefix + keySuffix);
    }

    private byte[][] getKeys(final String... keySuffixes) {
        return Arrays.stream(keySuffixes)
                .filter(BaseRedisRepository::isNotNullNorEmptyNorBlank)
                .map(this::getKey)
                .toArray(byte[][]::new);
    }

    private List<T> getByKeys(final byte[]... keys) {
        final var values = jedis.mget(keys);
        return values.stream()
                .filter(BaseRedisRepository::isNotNullNorEmpty)
                .map(this::convertFrom)
                .collect(Collectors.toList());
    }

    private byte[][] getAllKeys() {
        return jedis.keys(allKeysPattern).toArray(byte[][]::new);
    }
}
