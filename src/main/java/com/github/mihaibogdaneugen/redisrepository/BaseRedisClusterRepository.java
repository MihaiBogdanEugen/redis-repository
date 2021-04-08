package com.github.mihaibogdaneugen.redisrepository;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisClusterCommand;
import redis.clients.jedis.JedisSlotBasedConnectionHandler;
import redis.clients.jedis.Response;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.jedis.params.SetParams;
import redis.clients.jedis.util.JedisClusterCRC16;
import redis.clients.jedis.util.SafeEncoder;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.github.mihaibogdaneugen.redisrepository.RedisRepositoryStrategy.EACH_ENTITY_IS_A_VALUE;
import static com.github.mihaibogdaneugen.redisrepository.RedisRepositoryStrategy.EACH_ENTITY_IS_A_VALUE_IN_A_HASH;

public abstract class BaseRedisClusterRepository<T> implements RedisClusterRepository<T> {

    private final JedisSlotBasedConnectionHandler jedisSlotBasedConnectionHandler;
    private final Consumer<JedisException> jedisExceptionInterceptor;
    private final int maxAttempts;
    private final RedisRepositoryStrategy strategy;
    private final RedisClusterRepositoryConfiguration<T> configuration;
    private final String keyPrefix;
    private final String parentKey;

    public BaseRedisClusterRepository(final RedisClusterRepositoryConfiguration<T> configuration) {
        throwIfNull(configuration, "configuration");
        this.jedisSlotBasedConnectionHandler = configuration.getJedisSlotBasedConnectionHandler();
        this.jedisExceptionInterceptor = configuration.getJedisExceptionInterceptor();
        this.maxAttempts = configuration.getMaxAttempts();
        this.strategy = configuration.getStrategy();
        this.configuration = configuration;
        switch (strategy) {
            case EACH_ENTITY_IS_A_VALUE:
            case EACH_ENTITY_IS_A_HASH:
                this.keyPrefix = configuration.getCollectionKey() + configuration.getKeySeparator();
                this.parentKey = null;
                break;
            case EACH_ENTITY_IS_A_VALUE_IN_A_HASH:
                this.keyPrefix = null;
                this.parentKey = configuration.getCollectionKey();
                break;
            default:
                throw new UnsupportedOperationException("Unsupported RedisRepositoryStrategy!");
        }
    }

    /**
     * Retrieves the entity with the given identifier.<br/>
     * Implementation details: <br/>
     * - If the strategy used is EACH_ENTITY_IS_A_VALUE, this method calls the GET Redis command.<br/>
     * - If the strategy used is EACH_ENTITY_IS_A_HASH, this method calls the HGETALL Redis command.<br/>
     * - If the strategy used is EACH_ENTITY_IS_A_VALUE_IN_A_HASH, this method calls the HGET Redis command.<br/>
     * @see <a href="https://redis.io/commands/GET">GET</a>
     * @see <a href="https://redis.io/commands/HGETALL">HGETALL</a>
     * @see <a href="https://redis.io/commands/HGET">HGET</a>
     * @param id The String identifier of the entity
     * @return Optional object, empty if no such entity is found, or the object otherwise
     */
    @Override
    public final Optional<T> get(final String id) {
        throwIfNullOrEmptyOrBlank(id, "id");
        switch (strategy) {
            case EACH_ENTITY_IS_A_VALUE:
                if (configuration.useBinaryApi()) {
                    final var key = SafeEncoder.encode(keyPrefix + id);
                    final var entity = runClusterBinaryCommand(key, jedis -> jedis.get(key));
                    return isNullOrEmpty(entity)
                            ? Optional.empty()
                            : Optional.of(configuration.binaryDeserialize(entity));
                } else {
                    final var key = keyPrefix + id;
                    final var entity = runClusterCommand(key, jedis -> jedis.get(key));
                    return isNullOrEmptyOrBlank(entity)
                            ? Optional.empty()
                            : Optional.of(configuration.deserialize(entity));
                }
            case EACH_ENTITY_IS_A_HASH:
                if (configuration.useBinaryApi()) {
                    final var key = SafeEncoder.encode(keyPrefix + id);
                    final var entity = runClusterBinaryCommand(key, jedis -> jedis.hgetAll(key));
                    return isNullOrEmpty(entity)
                            ? Optional.empty()
                            : Optional.of(configuration.binaryDeserializeFromHash(entity));
                } else {
                    final var key = keyPrefix + id;
                    final var entity = runClusterCommand(key, jedis -> jedis.hgetAll(key));
                    return isNullOrEmpty(entity)
                            ? Optional.empty()
                            : Optional.of(configuration.deserializeFromHash(entity));
                }
            case EACH_ENTITY_IS_A_VALUE_IN_A_HASH:
                if (configuration.useBinaryApi()) {
                    final var key = SafeEncoder.encode(parentKey);
                    final var entity = runClusterBinaryCommand(key, jedis -> jedis.hget(key, SafeEncoder.encode(id)));
                    return isNullOrEmpty(entity)
                            ? Optional.empty()
                            : Optional.of(configuration.binaryDeserialize(entity));
                } else {
                    final var entity = runClusterCommand(parentKey, jedis -> jedis.hget(parentKey, id));
                    return isNullOrEmptyOrBlank(entity)
                            ? Optional.empty()
                            : Optional.of(configuration.deserialize(entity));
                }
            default:
                throw new UnsupportedOperationException("Unsupported RedisRepositoryStrategy!");
        }
    }

    /**
     * Retrieves the entities with the given identifiers.<br/>
     * Warning: For the EACH_ENTITY_IS_A_HASH strategy, this method does batch-getting by given identifiers, so, it might offer a poor overall performance.<br/>
     * Implementation details: <br/>
     * - If the strategy used is EACH_ENTITY_IS_A_VALUE, this method calls the MGET Redis command.<br/>
     * - If the strategy used is EACH_ENTITY_IS_A_HASH, this method calls the KEYS, HGETALL and SYNC Redis commands, as well as pipelining.<br/>
     * - If the strategy used is EACH_ENTITY_IS_A_VALUE_IN_A_HASH, this method calls the HMGET Redis command.<br/>
     * @see <a href="https://redis.io/commands/MGET">MGET</a>
     * @see <a href="https://redis.io/commands/KEYS">KEYS</a>
     * @see <a href="https://redis.io/commands/HGETALL">HGETALL</a>
     * @see <a href="https://redis.io/commands/SYNC">SYNC</a>
     * @see <a href="https://redis.io/commands/HMGET">HMGET</a>
     * @see <a href="https://redis.io/topics/pipelining">pipelining</a>
     * @param ids The set of Strings identifiers of entities
     * @return A set of entities
     */
    @Override
    public final Set<T> get(final Set<String> ids) {
        throwIfNullOrEmpty(ids, "ids");
        switch (strategy) {
            case EACH_ENTITY_IS_A_VALUE:
                if (configuration.useBinaryApi()) {
                    final var keys = ids.stream()
                            .filter(Predicate.not(BaseRedisClusterRepository::isNullOrEmptyOrBlank))
                            .map(id -> keyPrefix + id)
                            .map(SafeEncoder::encode)
                            .collect(Collectors.toSet());
                    final var values = runClusterBinaryCommand(keys, jedis -> jedis.mget(keys.toArray(byte[][]::new)));
                    return values.stream()
                            .filter(Predicate.not(BaseRedisClusterRepository::isNullOrEmpty))
                            .map(configuration::binaryDeserialize)
                            .collect(Collectors.toSet());
                } else {
                    final var keys = ids.stream()
                            .filter(Predicate.not(BaseRedisClusterRepository::isNullOrEmptyOrBlank))
                            .map(id -> keyPrefix + id)
                            .collect(Collectors.toSet());
                    final var slots = getSlots(keys);
                    final var values = new ArrayList<String>();
                    slots.values().forEach(keysInSlot -> {
                        final var keysInSlotAsArray = keysInSlot.toArray(String[]::new);
                        final var valuesInSlot = runClusterCommand(keysInSlot, jedis -> jedis.mget(keysInSlotAsArray));
                        values.addAll(valuesInSlot);
                    });
                    return values.stream()
                            .filter(Predicate.not(BaseRedisClusterRepository::isNullOrEmptyOrBlank))
                            .map(configuration::deserialize)
                            .collect(Collectors.toSet());
                }
            case EACH_ENTITY_IS_A_HASH:
                if (configuration.useBinaryApi()) {
                    final var keys = ids.stream()
                            .filter(Predicate.not(BaseRedisClusterRepository::isNullOrEmptyOrBlank))
                            .map(id -> SafeEncoder.encode(keyPrefix + id))
                            .collect(Collectors.toSet());
                    return runClusterBinaryCommand(keys, jedis -> {
                        final var responses = new ArrayList<Response<Map<byte[], byte[]>>>();
                        try (final var pipeline = jedis.pipelined()) {
                            keys.forEach(key -> responses.add(pipeline.hgetAll(key)));
                            pipeline.sync();
                        }
                        return responses.stream()
                                .map(Response::get)
                                .filter(Predicate.not(BaseRedisClusterRepository::isNullOrEmpty))
                                .map(configuration::binaryDeserializeFromHash)
                                .collect(Collectors.toSet());
                    });
                } else {
                    final var keys = ids.stream()
                            .filter(Predicate.not(BaseRedisClusterRepository::isNullOrEmptyOrBlank))
                            .map(id -> keyPrefix + id)
                            .collect(Collectors.toSet());
                    final var slots = getSlots(keys);
                    final var values = new HashSet<T>();
                    slots.values().forEach(keysInSlot -> {
                        final var valuesInSlot = runClusterCommand(keysInSlot, jedis -> {
                            final var responses = new ArrayList<Response<Map<String, String>>>();
                            try (final var pipeline = jedis.pipelined()) {
                                keysInSlot.forEach(key -> responses.add(pipeline.hgetAll(key)));
                                pipeline.sync();
                            }
                            return responses.stream()
                                    .map(Response::get)
                                    .filter(Predicate.not(BaseRedisClusterRepository::isNullOrEmpty))
                                    .map(configuration::deserializeFromHash)
                                    .collect(Collectors.toSet());
                        });
                        values.addAll(valuesInSlot);
                    });
                    return values;
                }
            case EACH_ENTITY_IS_A_VALUE_IN_A_HASH:
                if (configuration.useBinaryApi()) {
                    final var idsArray = ids.stream()
                            .filter(Predicate.not(BaseRedisClusterRepository::isNullOrEmptyOrBlank))
                            .map(SafeEncoder::encode)
                            .toArray(byte[][]::new);
                    final var key = SafeEncoder.encode(parentKey);
                    final var entities = runClusterBinaryCommand(key, jedis -> jedis.hmget(key, idsArray));
                    return entities.stream()
                            .filter(Predicate.not(BaseRedisClusterRepository::isNullOrEmpty))
                            .map(configuration::binaryDeserialize)
                            .collect(Collectors.toSet());
                } else {
                    final var idsArray = ids.stream()
                            .filter(Predicate.not(BaseRedisClusterRepository::isNullOrEmptyOrBlank))
                            .toArray(String[]::new);
                    final var entities = runClusterCommand(parentKey, jedis -> jedis.hmget(parentKey, idsArray));
                    return entities.stream()
                            .filter(Predicate.not(BaseRedisClusterRepository::isNullOrEmptyOrBlank))
                            .map(configuration::deserialize)
                            .collect(Collectors.toSet());
                }
            default:
                throw new UnsupportedOperationException("Unsupported RedisRepositoryStrategy!");
        }
    }

    /**
     * Retrieves all entities from the current collection.<br/>
     * Warning: for the EACH_ENTITY_IS_A_VALUE and EACH_ENTITY_IS_A_HASH strategies, this method is not implemented.<br/>
     * Implementation details: <br/>
     * - If the strategy used is EACH_ENTITY_IS_A_VALUE_IN_A_HASH, this method calls the HGETALL Redis command.<br/>
     * @see <a href="https://redis.io/commands/HGETALL">HGETALL</a>
     * @return A set of entities
     */
    @Override
    public final Set<T> getAll() {
        switch (strategy) {
            case EACH_ENTITY_IS_A_VALUE_IN_A_HASH:
                if (configuration.useBinaryApi()) {
                    final var key = SafeEncoder.encode(parentKey);
                    return runClusterBinaryCommand(key, jedis -> jedis.hgetAll(SafeEncoder.encode(parentKey)).values().stream()
                            .filter(Predicate.not(BaseRedisClusterRepository::isNullOrEmpty))
                            .map(configuration::binaryDeserialize)
                            .collect(Collectors.toSet()));
                } else {
                    return runClusterCommand(parentKey, jedis -> jedis.hgetAll(parentKey).values().stream()
                            .filter(Predicate.not(BaseRedisClusterRepository::isNullOrEmptyOrBlank))
                            .map(configuration::deserialize)
                            .collect(Collectors.toSet()));
                }
            case EACH_ENTITY_IS_A_VALUE:
            case EACH_ENTITY_IS_A_HASH:
            default:
                throw new UnsupportedOperationException("Unsupported RedisRepositoryStrategy!");
        }
    }

    /**
     * Checks if the entity with the specified identifier exists in the repository or not.<br/>
     * Implementation details: <br/>
     * - If the strategy used is EACH_ENTITY_IS_A_VALUE, this method calls the EXISTS Redis command.<br/>
     * - If the strategy used is EACH_ENTITY_IS_A_HASH, this method calls the EXISTS Redis command.<br/>
     * - If the strategy used is EACH_ENTITY_IS_A_VALUE_IN_A_HASH, this method calls the HEXISTS Redis command.<br/>
     * @see <a href="https://redis.io/commands/EXISTS">EXISTS</a>
     * @see <a href="https://redis.io/commands/HEXISTS">HEXISTS</a>
     * @param id The String identifier of the entity
     * @return A Boolean object, true if it exists, false otherwise
     */
    @Override
    public final Boolean exists(final String id) {
        throwIfNullOrEmptyOrBlank(id, "id");
        switch (strategy) {
            case EACH_ENTITY_IS_A_VALUE:
            case EACH_ENTITY_IS_A_HASH:
                if (configuration.useBinaryApi()) {
                    final var key = SafeEncoder.encode(keyPrefix + id);
                    return runClusterBinaryCommand(key, jedis -> jedis.exists(key));
                } else {
                    final var key = keyPrefix + id;
                    return runClusterCommand(key, jedis -> jedis.exists(key));
                }
            case EACH_ENTITY_IS_A_VALUE_IN_A_HASH:
                if (configuration.useBinaryApi()) {
                    final var key = SafeEncoder.encode(parentKey);
                    return runClusterBinaryCommand(key, jedis -> jedis.hexists(key, SafeEncoder.encode(id)));
                } else {
                    return runClusterCommand(parentKey, jedis -> jedis.hexists(parentKey, id));
                }
            default:
                throw new UnsupportedOperationException("Unsupported RedisRepositoryStrategy!");
        }
    }

    /**
     * Sets (updates or inserts) the given entity with the specified identifier.<br/>
     * Implementation details: <br/>
     * - If the strategy used is EACH_ENTITY_IS_A_VALUE, this method calls the SET Redis command.<br/>
     * - If the strategy used is EACH_ENTITY_IS_A_HASH, this method calls the HSET Redis command.<br/>
     * - If the strategy used is EACH_ENTITY_IS_A_VALUE_IN_A_HASH, this method calls the HSET Redis command.<br/>
     * @see <a href="https://redis.io/commands/SET">SET</a>
     * @see <a href="https://redis.io/commands/HSET">HSET</a>
     * @param id The String identifier of the entity
     * @param entity The entity to be set
     */
    @Override
    public final void set(final String id, final T entity) {
        throwIfNullOrEmptyOrBlank(id, "id");
        throwIfNull(entity, "entity");
        switch (strategy) {
            case EACH_ENTITY_IS_A_VALUE:
                if (configuration.useBinaryApi()) {
                    final var key = SafeEncoder.encode(keyPrefix + id);
                    runClusterBinaryCommand(key, jedis -> jedis.set(key, configuration.binarySerialize(entity)));
                } else {
                    final var key = keyPrefix + id;
                    runClusterCommand(key, jedis -> jedis.set(key, configuration.serialize(entity)));
                }
                break;
            case EACH_ENTITY_IS_A_HASH:
                if (configuration.useBinaryApi()) {
                    final var key = SafeEncoder.encode(keyPrefix + id);
                    runClusterBinaryCommand(key, jedis -> jedis.hset(key, configuration.binarySerializeToHash(entity)));
                } else {
                    final var key = keyPrefix + id;
                    runClusterCommand(key, jedis -> jedis.hset(key, configuration.serializeToHash(entity)));
                }
                break;
            case EACH_ENTITY_IS_A_VALUE_IN_A_HASH:
                if (configuration.useBinaryApi()) {
                    final var key = SafeEncoder.encode(parentKey);
                    runClusterBinaryCommand(key, jedis -> jedis.hset(key, SafeEncoder.encode(id), configuration.binarySerialize(entity)));
                } else {
                    runClusterCommand(parentKey, jedis -> jedis.hset(parentKey, id, configuration.serialize(entity)));
                }
                break;
            default:
                throw new UnsupportedOperationException("Unsupported RedisRepositoryStrategy!");
        }
    }

    /**
     * Sets the given entity with the specified identifier only if it does exist (update).<br/>
     * For the EACH_ENTITY_IS_A_VALUE strategy, a simple SET command with SetParams is used.<br/>
     * For both the EACH_ENTITY_IS_A_HASH and EACH_ENTITY_IS_A_VALUE_IN_A_HASH strategies, this method is providing a transactional behaviour by watching for the specific key.<br/>
     * Warning: For the EACH_ENTITY_IS_A_VALUE_IN_A_HASH, the watch is set on the parentKey - on the whole collection.<br/>
     * Implementation details: <br/>
     * - If the strategy used is EACH_ENTITY_IS_A_VALUE, this method calls the SET Redis command.<br/>
     * - If the strategy used is EACH_ENTITY_IS_A_HASH, this method calls the WATCH, EXISTS, UNWATCH, MULTI, HSET and EXEC Redis commands.<br/>
     * - If the strategy used is EACH_ENTITY_IS_A_VALUE_IN_A_HASH, this method calls the WATCH, HEXISTS, UNWATCH, MULTI, HSET and EXEC Redis commands.<br/>
     * @see <a href="https://redis.io/commands/SET">SET</a>
     * @see <a href="https://redis.io/commands/WATCH">WATCH</a>
     * @see <a href="https://redis.io/commands/UNWATCH">UNWATCH</a>
     * @see <a href="https://redis.io/commands/EXISTS">EXISTS</a>
     * @see <a href="https://redis.io/commands/HEXISTS">HEXISTS</a>
     * @see <a href="https://redis.io/commands/MULTI">MULTI</a>
     * @see <a href="https://redis.io/commands/HSET">HSET</a>
     * @see <a href="https://redis.io/commands/EXEC">EXEC</a>
     * @param id The String identifier of the entity
     * @param entity The entity to be set
     */
    @Override
    public final void setIfItDoesExist(final String id, final T entity) {
        throwIfNullOrEmptyOrBlank(id, "id");
        throwIfNull(entity, "entity");
        switch (strategy) {
            case EACH_ENTITY_IS_A_VALUE:
                if (configuration.useBinaryApi()) {
                    final var key = SafeEncoder.encode(keyPrefix + id);
                    runClusterBinaryCommand(key, jedis -> jedis.set(key, configuration.binarySerialize(entity), SetParams.setParams().xx()));
                } else {
                    final var key = keyPrefix + id;
                    runClusterCommand(key, jedis -> jedis.set(key, configuration.serialize(entity), SetParams.setParams().xx()));
                }
                break;
            case EACH_ENTITY_IS_A_HASH:
                if (configuration.useBinaryApi()) {
                    final var key = SafeEncoder.encode(keyPrefix + id);
                    runClusterBinaryCommand(key, jedis -> {
                        jedis.watch(key);
                        if (!jedis.exists(key)) {
                            jedis.unwatch();
                        } else {
                            try (final var transaction = jedis.multi()) {
                                transaction.hset(key, configuration.binarySerializeToHash(entity));
                                transaction.exec();
                            }
                        }
                        return null;
                    });
                } else {
                    final var key = keyPrefix + id;
                    runClusterCommand(key, jedis -> {
                        jedis.watch(key);
                        if (!jedis.exists(key)) {
                            jedis.unwatch();
                        } else {
                            try (final var transaction = jedis.multi()) {
                                transaction.hset(key, configuration.serializeToHash(entity));
                                transaction.exec();
                            }
                        }
                        return null;
                    });
                }
                break;
            case EACH_ENTITY_IS_A_VALUE_IN_A_HASH:
                if (configuration.useBinaryApi()) {
                    final var key = SafeEncoder.encode(parentKey);
                    runClusterBinaryCommand(key, jedis -> {
                        jedis.watch(key);
                        if (!jedis.hexists(key, SafeEncoder.encode(id))) {
                            jedis.unwatch();
                        } else {
                            try (final var transaction = jedis.multi()) {
                                transaction.hset(key, SafeEncoder.encode(id), configuration.binarySerialize(entity));
                                transaction.exec();
                            }
                        }
                        return null;
                    });
                } else {
                    runClusterCommand(parentKey, jedis -> {
                        jedis.watch(parentKey);
                        if (!jedis.hexists(parentKey, id)) {
                            jedis.unwatch();
                        } else {
                            try (final var transaction = jedis.multi()) {
                                transaction.hset(parentKey, id, configuration.serialize(entity));
                                transaction.exec();
                            }
                        }
                        return null;
                    });
                }
                break;
            default:
                throw new UnsupportedOperationException("Unsupported RedisRepositoryStrategy!");
        }
    }

    /**
     * Sets the given entity with the specified identifier only if it does not exist (insert).<br/>
     * For both the EACH_ENTITY_IS_A_VALUE and EACH_ENTITY_IS_A_VALUE_IN_A_HASH strategies, simple SETNX or HSETNX commands are used.<br/>
     * For the EACH_ENTITY_IS_A_HASH strategy, this method is providing a transactional behaviour by watching for the specific key.<br/>
     * Implementation details: <br/>
     * - If the strategy used is EACH_ENTITY_IS_A_VALUE, this method calls the SET Redis command.<br/>
     * - If the strategy used is EACH_ENTITY_IS_A_HASH, this method calls the WATCH, EXISTS, UNWATCH, MULTI, HSET and EXEC Redis commands.<br/>
     * - If the strategy used is EACH_ENTITY_IS_A_VALUE_IN_A_HASH, this method calls the HSETNX Redis command.<br/>
     * @see <a href="https://redis.io/commands/SET">SET</a>
     * @see <a href="https://redis.io/commands/WATCH">WATCH</a>
     * @see <a href="https://redis.io/commands/UNWATCH">UNWATCH</a>
     * @see <a href="https://redis.io/commands/EXISTS">EXISTS</a>
     * @see <a href="https://redis.io/commands/MULTI">MULTI</a>
     * @see <a href="https://redis.io/commands/HSET">HSET</a>
     * @see <a href="https://redis.io/commands/EXEC">EXEC</a>
     * @see <a href="https://redis.io/commands/HSETNX">HSETNX</a>
     * @param id The String identifier of the entity
     * @param entity The entity to be set
     */
    @Override
    public final void setIfItDoesNotExist(final String id, final T entity) {
        throwIfNullOrEmptyOrBlank(id, "id");
        throwIfNull(entity, "entity");
        switch (strategy) {
            case EACH_ENTITY_IS_A_VALUE:
                if (configuration.useBinaryApi()) {
                    final var key = SafeEncoder.encode(keyPrefix + id);
                    runClusterBinaryCommand(key, jedis -> jedis.setnx(key, configuration.binarySerialize(entity)));
                } else {
                    final var key = keyPrefix + id;
                    runClusterCommand(key, jedis -> jedis.setnx(key, configuration.serialize(entity)));
                }
                break;
            case EACH_ENTITY_IS_A_HASH:
                if (configuration.useBinaryApi()) {
                    final var key = SafeEncoder.encode(keyPrefix + id);
                    runClusterBinaryCommand(key, jedis -> {
                        jedis.watch(key);
                        if (jedis.exists(key)) {
                            jedis.unwatch();
                        } else {
                            try (final var transaction = jedis.multi()) {
                                transaction.hset(key, configuration.binarySerializeToHash(entity));
                                transaction.exec();
                            }
                        }
                        return null;
                    });
                } else {
                    final var key = keyPrefix + id;
                    runClusterCommand(key, jedis -> {
                        jedis.watch(key);
                        if (jedis.exists(key)) {
                            jedis.unwatch();
                        } else {
                            try (final var transaction = jedis.multi()) {
                                transaction.hset(key, configuration.serializeToHash(entity));
                                transaction.exec();
                            }
                        }
                        return null;
                    });
                }
                break;
            case EACH_ENTITY_IS_A_VALUE_IN_A_HASH:
                if (configuration.useBinaryApi()) {
                    final var key = SafeEncoder.encode(parentKey);
                    runClusterBinaryCommand(key, jedis -> jedis.hsetnx(key, SafeEncoder.encode(id), configuration.binarySerialize(entity)));
                } else {
                    runClusterCommand(parentKey, jedis -> jedis.hsetnx(parentKey, id, configuration.serialize(entity)));
                }
                break;
            default:
                throw new UnsupportedOperationException("Unsupported RedisRepositoryStrategy!");
        }
    }

    /**
     * Updates the entity with the specified identifier by calling the updater.<br/>
     * This method provides a transactional behaviour for updating the entity, by watching the specific key.<br/>
     * Warning: For the EACH_ENTITY_IS_A_VALUE_IN_A_HASH, the watch is set on the parentKey - on the whole collection.<br/>
     * Implementation details: <br/>
     * - If the strategy used is EACH_ENTITY_IS_A_VALUE, this method calls the WATCH, GET, UNWATCH, MULTI, SET and EXEC Redis commands.<br/>
     * - If the strategy used is EACH_ENTITY_IS_A_HASH, this method calls the WATCH, HGETALL, UNWATCH, MULTI, HSET and EXEC Redis commands.<br/>
     * - If the strategy used is EACH_ENTITY_IS_A_VALUE_IN_A_HASH, this method calls the WATCH, HGET, UNWATCH, MULTI, HSET and EXEC Redis commands.<br/>
     * Note: This method calls the WATCH, GET, UNWATCH, MULTI, DEL and EXEC Redis commands.
     * @see <a href="https://redis.io/commands/WATCH">WATCH</a>
     * @see <a href="https://redis.io/commands/GET">GET</a>
     * @see <a href="https://redis.io/commands/HGETALL">HGETALL</a>
     * @see <a href="https://redis.io/commands/HGET">HGET</a>
     * @see <a href="https://redis.io/commands/UNWATCH">UNWATCH</a>
     * @see <a href="https://redis.io/commands/MULTI">MULTI</a>
     * @see <a href="https://redis.io/commands/SET">SET</a>
     * @see <a href="https://redis.io/commands/HSET">HSET</a>
     * @see <a href="https://redis.io/commands/EXEC">EXEC</a>
     * @param id The String identifier of the entity
     * @param updater A function that updates the entity
     * @return Optional object, empty if no such entity exists, or boolean value indicating the status of the transaction
     */
    @Override
    public final Optional<Boolean> update(final String id, final Function<T, T> updater) {
        throwIfNullOrEmptyOrBlank(id, "id");
        throwIfNull(updater, "updater");
        switch (strategy) {
            case EACH_ENTITY_IS_A_VALUE:
                if (configuration.useBinaryApi()) {
                    final var key = SafeEncoder.encode(keyPrefix + id);
                    return runClusterBinaryCommand(key, jedis -> {
                        jedis.watch(key);
                        final var value = jedis.get(key);
                        if (isNullOrEmpty(value)) {
                            jedis.unwatch();
                            return Optional.empty();
                        }
                        final var entity = configuration.binaryDeserialize(value);
                        final var newEntity = updater.apply(entity);
                        final var newValue = configuration.binarySerialize(newEntity);
                        final List<Object> results;
                        try (final var transaction = jedis.multi()) {
                            transaction.set(key, newValue);
                            results = transaction.exec();
                        }
                        return Optional.of(!isNullOrEmpty(results));
                    });
                } else {
                    final var key = keyPrefix + id;
                    return runClusterCommand(key, jedis -> {
                        jedis.watch(key);
                        final var value = jedis.get(key);
                        if (isNullOrEmptyOrBlank(value)) {
                            jedis.unwatch();
                            return Optional.empty();
                        }
                        final var entity = configuration.deserialize(value);
                        final var newEntity = updater.apply(entity);
                        final var newValue = configuration.serialize(newEntity);
                        final List<Object> results;
                        try (final var transaction = jedis.multi()) {
                            transaction.set(key, newValue);
                            results = transaction.exec();
                        }
                        return Optional.of(!isNullOrEmpty(results));
                    });
                }
            case EACH_ENTITY_IS_A_HASH:
                if (configuration.useBinaryApi()) {
                    final var key = SafeEncoder.encode(keyPrefix + id);
                    return runClusterBinaryCommand(key, jedis -> {
                        jedis.watch(key);
                        final var value = jedis.hgetAll(key);
                        if (isNullOrEmpty(value)) {
                            jedis.unwatch();
                            return Optional.empty();
                        }
                        final var entity = configuration.binaryDeserializeFromHash(value);
                        final var newEntity = updater.apply(entity);
                        final var newValue = configuration.binarySerializeToHash(newEntity);
                        final List<Object> results;
                        try (final var transaction = jedis.multi()) {
                            transaction.hset(key, newValue);
                            results = transaction.exec();
                        }
                        return Optional.of(!isNullOrEmpty(results));
                    });
                } else {
                    final var key = keyPrefix + id;
                    return runClusterCommand(key, jedis -> {
                        jedis.watch(key);
                        final var value = jedis.hgetAll(key);
                        if (isNullOrEmpty(value)) {
                            jedis.unwatch();
                            return Optional.empty();
                        }
                        final var entity = configuration.deserializeFromHash(value);
                        final var newEntity = updater.apply(entity);
                        final var newValue = configuration.serializeToHash(newEntity);
                        final List<Object> results;
                        try (final var transaction = jedis.multi()) {
                            transaction.hset(key, newValue);
                            results = transaction.exec();
                        }
                        return Optional.of(!isNullOrEmpty(results));
                    });
                }
            case EACH_ENTITY_IS_A_VALUE_IN_A_HASH:
                if (configuration.useBinaryApi()) {
                    final var key = SafeEncoder.encode(parentKey);
                    return runClusterBinaryCommand(key, jedis -> {
                        jedis.watch(key);
                        final var value = jedis.hget(key, SafeEncoder.encode(id));
                        if (isNullOrEmpty(value)) {
                            jedis.unwatch();
                            return Optional.empty();
                        }
                        final var entity = configuration.binaryDeserialize(value);
                        final var newEntity = updater.apply(entity);
                        final var newValue = configuration.binarySerialize(newEntity);
                        final List<Object> results;
                        try (final var transaction = jedis.multi()) {
                            transaction.hset(key, SafeEncoder.encode(id), newValue);
                            results = transaction.exec();
                        }
                        return Optional.of(!isNullOrEmpty(results));
                    });
                } else {
                    return runClusterCommand(parentKey, jedis -> {
                        jedis.watch(parentKey);
                        final var value = jedis.hget(parentKey, id);
                        if (isNullOrEmptyOrBlank(value)) {
                            jedis.unwatch();
                            return Optional.empty();
                        }
                        final var entity = configuration.deserialize(value);
                        final var newEntity = updater.apply(entity);
                        final var newValue = configuration.serialize(newEntity);
                        final List<Object> results;
                        try (final var transaction = jedis.multi()) {
                            transaction.hset(parentKey, id, newValue);
                            results = transaction.exec();
                        }
                        return Optional.of(!isNullOrEmpty(results));
                    });
                }
            default:
                throw new UnsupportedOperationException("Unsupported RedisRepositoryStrategy!");
        }
    }

    /**
     * Updates the entity with the specified identifier by calling the updater only if the condition returns true (conditional update).<br/>
     * This method provides a transactional behaviour for updating the entity, by watching the specific key.<br/>
     * Warning: For the EACH_ENTITY_IS_A_VALUE_IN_A_HASH, the watch is set on the parentKey - on the whole collection.<br/>
     * Implementation details: <br/>
     * - If the strategy used is EACH_ENTITY_IS_A_VALUE, this method calls the WATCH, GET, UNWATCH, MULTI, SET and EXEC Redis commands.<br/>
     * - If the strategy used is EACH_ENTITY_IS_A_HASH, this method calls the WATCH, HGETALL, UNWATCH, MULTI, HSET and EXEC Redis commands.<br/>
     * - If the strategy used is EACH_ENTITY_IS_A_VALUE_IN_A_HASH, this method calls the WATCH, HGET, UNWATCH, MULTI, HSET and EXEC Redis commands.<br/>
     * Note: This method calls the WATCH, GET, UNWATCH, MULTI, DEL and EXEC Redis commands.
     * @see <a href="https://redis.io/commands/WATCH">WATCH</a>
     * @see <a href="https://redis.io/commands/GET">GET</a>
     * @see <a href="https://redis.io/commands/HGETALL">HGETALL</a>
     * @see <a href="https://redis.io/commands/HGET">HGET</a>
     * @see <a href="https://redis.io/commands/UNWATCH">UNWATCH</a>
     * @see <a href="https://redis.io/commands/MULTI">MULTI</a>
     * @see <a href="https://redis.io/commands/SET">SET</a>
     * @see <a href="https://redis.io/commands/HSET">HSET</a>
     * @see <a href="https://redis.io/commands/EXEC">EXEC</a>
     * @param id The String identifier of the entity
     * @param updater A function that updates the entity
     * @param condition A function that represents the condition for the update to happen
     * @return Optional object, empty if no such entity exists, or boolean value indicating the status of the transaction
     */
    @Override
    public final Optional<Boolean> update(final String id, final Function<T, T> updater, final Function<T, Boolean> condition) {
        throwIfNullOrEmptyOrBlank(id, "id");
        throwIfNull(updater, "updater");
        throwIfNull(condition, "condition");
        switch (strategy) {
            case EACH_ENTITY_IS_A_VALUE:
                if (configuration.useBinaryApi()) {
                    final var key = SafeEncoder.encode(keyPrefix + id);
                    return runClusterBinaryCommand(key, jedis -> {
                        jedis.watch(key);
                        final var value = jedis.get(key);
                        if (isNullOrEmpty(value)) {
                            jedis.unwatch();
                            return Optional.empty();
                        }
                        final var entity = configuration.binaryDeserialize(value);
                        if (!condition.apply(entity)) {
                            jedis.unwatch();
                            return Optional.of(true);
                        }
                        final var newEntity = updater.apply(entity);
                        final var newValue = configuration.binarySerialize(newEntity);
                        final List<Object> results;
                        try (final var transaction = jedis.multi()) {
                            transaction.set(key, newValue);
                            results = transaction.exec();
                        }
                        return Optional.of(!isNullOrEmpty(results));
                    });
                } else {
                    final var key = keyPrefix + id;
                    return runClusterCommand(key, jedis -> {
                        jedis.watch(key);
                        final var value = jedis.get(key);
                        if (isNullOrEmptyOrBlank(value)) {
                            jedis.unwatch();
                            return Optional.empty();
                        }
                        final var entity = configuration.deserialize(value);
                        if (!condition.apply(entity)) {
                            jedis.unwatch();
                            return Optional.of(true);
                        }
                        final var newEntity = updater.apply(entity);
                        final var newValue = configuration.serialize(newEntity);
                        final List<Object> results;
                        try (final var transaction = jedis.multi()) {
                            transaction.set(key, newValue);
                            results = transaction.exec();
                        }
                        return Optional.of(!isNullOrEmpty(results));
                    });
                }
            case EACH_ENTITY_IS_A_HASH:
                if (configuration.useBinaryApi()) {
                    final var key = SafeEncoder.encode(keyPrefix + id);
                    return runClusterBinaryCommand(key, jedis -> {
                        jedis.watch(key);
                        final var value = jedis.hgetAll(key);
                        if (isNullOrEmpty(value)) {
                            jedis.unwatch();
                            return Optional.empty();
                        }
                        final var entity = configuration.binaryDeserializeFromHash(value);
                        if (!condition.apply(entity)) {
                            jedis.unwatch();
                            return Optional.of(true);
                        }
                        final var newEntity = updater.apply(entity);
                        final var newValue = configuration.binarySerializeToHash(newEntity);
                        final List<Object> results;
                        try (final var transaction = jedis.multi()) {
                            transaction.hset(key, newValue);
                            results = transaction.exec();
                        }
                        return Optional.of(!isNullOrEmpty(results));
                    });
                } else {
                    final var key = keyPrefix + id;
                    return runClusterCommand(key, jedis -> {
                        jedis.watch(key);
                        final var value = jedis.hgetAll(key);
                        if (isNullOrEmpty(value)) {
                            jedis.unwatch();
                            return Optional.empty();
                        }
                        final var entity = configuration.deserializeFromHash(value);
                        if (!condition.apply(entity)) {
                            jedis.unwatch();
                            return Optional.of(true);
                        }
                        final var newEntity = updater.apply(entity);
                        final var newValue = configuration.serializeToHash(newEntity);
                        final List<Object> results;
                        try (final var transaction = jedis.multi()) {
                            transaction.hset(key, newValue);
                            results = transaction.exec();
                        }
                        return Optional.of(!isNullOrEmpty(results));
                    });
                }
            case EACH_ENTITY_IS_A_VALUE_IN_A_HASH:
                if (configuration.useBinaryApi()) {
                    final var key = SafeEncoder.encode(parentKey);
                    return runClusterBinaryCommand(key, jedis -> {
                        jedis.watch(key);
                        final var value = jedis.hget(key, SafeEncoder.encode(id));
                        if (isNullOrEmpty(value)) {
                            jedis.unwatch();
                            return Optional.empty();
                        }
                        final var entity = configuration.binaryDeserialize(value);
                        if (!condition.apply(entity)) {
                            jedis.unwatch();
                            return Optional.of(true);
                        }
                        final var newEntity = updater.apply(entity);
                        final var newValue = configuration.binarySerialize(newEntity);
                        final List<Object> results;
                        try (final var transaction = jedis.multi()) {
                            transaction.hset(key, SafeEncoder.encode(id), newValue);
                            results = transaction.exec();
                        }
                        return Optional.of(!isNullOrEmpty(results));
                    });
                } else {
                    return runClusterCommand(parentKey, jedis -> {
                        jedis.watch(parentKey);
                        final var value = jedis.hget(parentKey, id);
                        if (isNullOrEmptyOrBlank(value)) {
                            jedis.unwatch();
                            return Optional.empty();
                        }
                        final var entity = configuration.deserialize(value);
                        if (!condition.apply(entity)) {
                            jedis.unwatch();
                            return Optional.of(true);
                        }
                        final var newEntity = updater.apply(entity);
                        final var newValue = configuration.serialize(newEntity);
                        final List<Object> results;
                        try (final var transaction = jedis.multi()) {
                            transaction.hset(parentKey, id, newValue);
                            results = transaction.exec();
                        }
                        return Optional.of(!isNullOrEmpty(results));
                    });
                }
            default:
                throw new UnsupportedOperationException("Unsupported RedisRepositoryStrategy!");
        }
    }

    /**
     * Removes the entity with the given identifier.<br/>
     * Implementation details: <br/>
     * - If the strategy used is EACH_ENTITY_IS_A_VALUE, this method calls the DEL Redis command.<br/>
     * - If the strategy used is EACH_ENTITY_IS_A_HASH, this method calls the DEL Redis command.<br/>
     * - If the strategy used is EACH_ENTITY_IS_A_VALUE_IN_A_HASH, this method calls theHDEL Redis command.<br/>
     * @see <a href="https://redis.io/commands/DEL">DEL</a>
     * @see <a href="https://redis.io/commands/HDEL">HDEL</a>
     * @param id The String identifier of the entity
     */
    @Override
    public final void delete(final String id) {
        throwIfNullOrEmptyOrBlank(id, "id");
        switch (strategy) {
            case EACH_ENTITY_IS_A_VALUE:
            case EACH_ENTITY_IS_A_HASH:
                if (configuration.useBinaryApi()) {
                    final var key = SafeEncoder.encode(keyPrefix + id);
                    runClusterBinaryCommand(key, jedis -> jedis.del(key));
                } else {
                    final var key = keyPrefix + id;
                    runClusterCommand(key, jedis -> jedis.del(key));
                }
                break;
            case EACH_ENTITY_IS_A_VALUE_IN_A_HASH:
                if (configuration.useBinaryApi()) {
                    final var key = SafeEncoder.encode(parentKey);
                    runClusterBinaryCommand(key, jedis -> jedis.hdel(key, SafeEncoder.encode(id)));
                } else {
                    runClusterCommand(parentKey, jedis -> jedis.hdel(parentKey, id));
                }
                break;
            default:
                throw new UnsupportedOperationException("Unsupported RedisRepositoryStrategy!");
        }
    }

    /**
     * Removes the entity with the given identifier only if the condition returns true (conditional delete).<br/>
     * This method provides a transactional behaviour for deleting the entity, by watching the specific key.<br/>
     * Warning: For the EACH_ENTITY_IS_A_VALUE_IN_A_HASH, the watch is set on the parentKey - on the whole collection.<br/>
     * Implementation details: <br/>
     * - If the strategy used is EACH_ENTITY_IS_A_VALUE, this method calls the WATCH, GET, UNWATCH, MULTI, DEL and EXEC Redis commands.<br/>
     * - If the strategy used is EACH_ENTITY_IS_A_HASH, this method calls the WATCH, HGETALL, UNWATCH, MULTI, DEL and EXEC Redis commands.<br/>
     * - If the strategy used is EACH_ENTITY_IS_A_VALUE_IN_A_HASH, this method calls the WATCH, HGET, UNWATCH, MULTI, HDEL and EXEC Redis commands.<br/>
     * Note: This method calls the WATCH, GET, UNWATCH, MULTI, DEL and EXEC Redis commands.
     * @see <a href="https://redis.io/commands/WATCH">WATCH</a>
     * @see <a href="https://redis.io/commands/GET">GET</a>
     * @see <a href="https://redis.io/commands/HGETALL">HGETALL</a>
     * @see <a href="https://redis.io/commands/HGET">HGET</a>
     * @see <a href="https://redis.io/commands/UNWATCH">UNWATCH</a>
     * @see <a href="https://redis.io/commands/MULTI">MULTI</a>
     * @see <a href="https://redis.io/commands/DEL">DEL</a>
     * @see <a href="https://redis.io/commands/HDEL">HDEL</a>
     * @see <a href="https://redis.io/commands/EXEC">EXEC</a>
     * @param id The String identifier of the entity
     * @param condition A function that represents the condition for the delete to happen
     @return Optional object, empty if no such entity exists, or boolean value indicating the status of the transaction
     */
    @Override
    public final Optional<Boolean> delete(final String id, final Function<T, Boolean> condition) {
        throwIfNullOrEmptyOrBlank(id, "id");
        throwIfNull(condition, "condition");
        switch (strategy) {
            case EACH_ENTITY_IS_A_VALUE:
                if (configuration.useBinaryApi()) {
                    final var key = SafeEncoder.encode(keyPrefix + id);
                    return runClusterBinaryCommand(key, jedis -> {
                        jedis.watch(key);
                        final var value = jedis.get(key);
                        if (isNullOrEmpty(value)) {
                            jedis.unwatch();
                            return Optional.empty();
                        }
                        final var entity = configuration.binaryDeserialize(value);
                        if (!condition.apply(entity)) {
                            jedis.unwatch();
                            return Optional.of(true);
                        }
                        final List<Object> results;
                        try (final var transaction = jedis.multi()) {
                            transaction.del(key);
                            results = transaction.exec();
                        }
                        return Optional.of(!isNullOrEmpty(results));
                    });
                } else {
                    final var key = keyPrefix + id;
                    return runClusterCommand(key, jedis -> {
                        jedis.watch(key);
                        final var value = jedis.get(key);
                        if (isNullOrEmptyOrBlank(value)) {
                            jedis.unwatch();
                            return Optional.empty();
                        }
                        final var entity = configuration.deserialize(value);
                        if (!condition.apply(entity)) {
                            jedis.unwatch();
                            return Optional.of(true);
                        }
                        final List<Object> results;
                        try (final var transaction = jedis.multi()) {
                            transaction.del(key);
                            results = transaction.exec();
                        }
                        return Optional.of(!isNullOrEmpty(results));
                    });
                }
            case EACH_ENTITY_IS_A_HASH:
                if (configuration.useBinaryApi()) {
                    final var key = SafeEncoder.encode(keyPrefix + id);
                    return runClusterBinaryCommand(key, jedis -> {
                        jedis.watch(key);
                        final var value = jedis.hgetAll(key);
                        if (isNullOrEmpty(value)) {
                            jedis.unwatch();
                            return Optional.empty();
                        }
                        final var entity = configuration.binaryDeserializeFromHash(value);
                        if (!condition.apply(entity)) {
                            jedis.unwatch();
                            return Optional.of(true);
                        }
                        final List<Object> results;
                        try (final var transaction = jedis.multi()) {
                            transaction.del(key);
                            results = transaction.exec();
                        }
                        return Optional.of(!isNullOrEmpty(results));
                    });
                } else {
                    final var key = keyPrefix + id;
                    return runClusterCommand(key, jedis -> {
                        jedis.watch(key);
                        final var value = jedis.hgetAll(key);
                        if (isNullOrEmpty(value)) {
                            jedis.unwatch();
                            return Optional.empty();
                        }
                        final var entity = configuration.deserializeFromHash(value);
                        if (!condition.apply(entity)) {
                            jedis.unwatch();
                            return Optional.of(true);
                        }
                        final List<Object> results;
                        try (final var transaction = jedis.multi()) {
                            transaction.del(key);
                            results = transaction.exec();
                        }
                        return Optional.of(!isNullOrEmpty(results));
                    });
                }
            case EACH_ENTITY_IS_A_VALUE_IN_A_HASH:
                if (configuration.useBinaryApi()) {
                    final var key = SafeEncoder.encode(parentKey);
                    return runClusterBinaryCommand(key, jedis -> {
                        jedis.watch(key);
                        final var value = jedis.hget(key, SafeEncoder.encode(id));
                        if (isNullOrEmpty(value)) {
                            jedis.unwatch();
                            return Optional.empty();
                        }
                        final var entity = configuration.binaryDeserialize(value);
                        if (!condition.apply(entity)) {
                            jedis.unwatch();
                            return Optional.of(true);
                        }
                        final List<Object> results;
                        try (final var transaction = jedis.multi()) {
                            transaction.hdel(key, SafeEncoder.encode(id));
                            results = transaction.exec();
                        }
                        return Optional.of(!isNullOrEmpty(results));
                    });
                } else {
                    return runClusterCommand(parentKey, jedis -> {
                        jedis.watch(parentKey);
                        final var value = jedis.hget(parentKey, id);
                        if (isNullOrEmptyOrBlank(value)) {
                            jedis.unwatch();
                            return Optional.empty();
                        }
                        final var entity = configuration.deserialize(value);
                        if (!condition.apply(entity)) {
                            jedis.unwatch();
                            return Optional.of(true);
                        }
                        final List<Object> results;
                        try (final var transaction = jedis.multi()) {
                            transaction.hdel(parentKey, id);
                            results = transaction.exec();
                        }
                        return Optional.of(!isNullOrEmpty(results));
                    });
                }
            default:
                throw new UnsupportedOperationException("Unsupported RedisRepositoryStrategy!");
        }
    }

    /**
     * Removes all entities with the given identifiers.<br/>
     * Implementation details: <br/>
     * - If the strategy used is EACH_ENTITY_IS_A_VALUE, this method calls the DEL Redis command.<br/>
     * - If the strategy used is EACH_ENTITY_IS_A_HASH, this method calls the DEL Redis command.<br/>
     * - If the strategy used is EACH_ENTITY_IS_A_VALUE_IN_A_HASH, this method calls the HDEL Redis command.<br/>
     * @see <a href="https://redis.io/commands/DEL">DEL</a>
     * @see <a href="https://redis.io/commands/HDEL">HDEL</a>
     * @param ids The set of Strings identifiers of entities
     */
    @Override
    public final void delete(final Set<String> ids) {
        throwIfNullOrEmpty(ids, "ids");
        switch (strategy) {
            case EACH_ENTITY_IS_A_VALUE:
            case EACH_ENTITY_IS_A_HASH:
                if (configuration.useBinaryApi()) {
                    final var keys = ids.stream()
                            .filter(Predicate.not(BaseRedisClusterRepository::isNullOrEmptyOrBlank))
                            .map(id -> SafeEncoder.encode(keyPrefix + id))
                            .collect(Collectors.toSet());
                    runClusterBinaryCommand(keys, jedis -> jedis.del(keys.toArray(byte[][]::new)));
                } else {
                    final var keys = ids.stream()
                            .filter(Predicate.not(BaseRedisClusterRepository::isNullOrEmptyOrBlank))
                            .map(id -> keyPrefix + id)
                            .collect(Collectors.toSet());
                    final var slots = getSlots(keys);
                    slots.values().forEach(keysInSlot -> {
                        final var keysInSlotArray = keysInSlot.toArray(String[]::new);
                        runClusterCommand(keysInSlot, jedis -> jedis.del(keysInSlotArray));
                    });
                }
                break;
            case EACH_ENTITY_IS_A_VALUE_IN_A_HASH:
                if (configuration.useBinaryApi()) {
                    final var key = SafeEncoder.encode(parentKey);
                    final var idsArray = ids.stream().map(SafeEncoder::encode).toArray(byte[][]::new);
                    runClusterBinaryCommand(key, jedis -> jedis.hdel(key, idsArray));
                } else {
                    final var idsArray = ids.toArray(String[]::new);
                    runClusterCommand(parentKey, jedis -> jedis.hdel(parentKey, idsArray));
                }
                break;
            default:
                throw new UnsupportedOperationException("Unsupported RedisRepositoryStrategy!");
        }
    }

    /**
     * Removes all entities from the current collection.<br/>
     * Warning: for the EACH_ENTITY_IS_A_VALUE and EACH_ENTITY_IS_A_HASH strategies, this method is not implemented.<br/>
     * Implementation details: <br/>
     * - If the strategy used is EACH_ENTITY_IS_A_VALUE_IN_A_HASH, this method calls the DEL Redis command.<br/>
     * @see <a href="https://redis.io/commands/DEL">DEL</a>
     */
    @Override
    public final void deleteAll() {
        switch (strategy) {
            case EACH_ENTITY_IS_A_VALUE_IN_A_HASH:
                if (configuration.useBinaryApi()) {
                    final var key = SafeEncoder.encode(parentKey);
                    runClusterBinaryCommand(key, jedis -> jedis.del(key));
                } else {
                    runClusterCommand(parentKey, jedis -> jedis.del(parentKey));
                }
                break;
            case EACH_ENTITY_IS_A_VALUE:
            case EACH_ENTITY_IS_A_HASH:
            default:
                throw new UnsupportedOperationException("Unsupported RedisRepositoryStrategy!");
        }
    }

    /**
     * Update the entity identified by the given identifier to the new provided value if its old value is equal with the given one.<br/>
     * This method is using a Lua script to do this in a transactional manner. <br/>
     * If the entity is not there, this method does nothing.<br/>
     * Warning: The EACH_ENTITY_IS_A_HASH strategy is not supported.<br/>
     * Implementation details: <br/>
     * - If the strategy used is EACH_ENTITY_IS_A_VALUE, the script calls the EXIST, GET and SET Redis commands.<br/>
     * - If the strategy used is EACH_ENTITY_IS_A_HASH, this method throws an unsupported operation exception.<br/>
     * - If the strategy used is EACH_ENTITY_IS_A_VALUE_IN_A_HASH, the script calls the HEXIST, HGET and HSET Redis commands.<br/>
     * @see <a href="https://redis.io/commands/EXIST">EXIST</a>
     * @see <a href="https://redis.io/commands/GET">GET</a>
     * @see <a href="https://redis.io/commands/SET">SET</a>
     * @see <a href="https://redis.io/commands/HEXIST">HEXIST</a>
     * @see <a href="https://redis.io/commands/HGET">HGET</a>
     * @see <a href="https://redis.io/commands/HSET">HSET</a>
     * @param id The String identifier of the entity
     * @param oldValue The old value of the entity
     * @param newValue The new value of the entity
     */
    @Override
    public final void updateIfItIs(final String id, final T oldValue, final T newValue) {
        throwIfNullOrEmptyOrBlank(id, "id");
        throwIfNull(oldValue, "oldValue");
        throwIfNull(newValue, "newValue");
        switch (strategy) {
            case EACH_ENTITY_IS_A_VALUE:
                final var scriptEachEntityIsAValue = LUA_SCRIPTS_UPDATE_IF_IT_IS.get(EACH_ENTITY_IS_A_VALUE);
                final var key = keyPrefix + id;
                if (configuration.useBinaryApi()) {
                    runClusterCommand(key, jedis -> jedis.eval(SafeEncoder.encode(scriptEachEntityIsAValue),
                            List.of(SafeEncoder.encode(key)),
                            List.of(configuration.binarySerialize(oldValue),
                                    configuration.binarySerialize(newValue))));
                } else {
                    runClusterCommand(key, jedis -> jedis.eval(scriptEachEntityIsAValue,
                            List.of(key),
                            List.of(configuration.serialize(oldValue),
                                    configuration.serialize(newValue))));
                }
                break;
            case EACH_ENTITY_IS_A_VALUE_IN_A_HASH:
                final var scriptEachEntityIsAValueInAHash = LUA_SCRIPTS_UPDATE_IF_IT_IS.get(EACH_ENTITY_IS_A_VALUE_IN_A_HASH);
                if (configuration.useBinaryApi()) {
                    runClusterCommand(parentKey, jedis -> jedis.eval(SafeEncoder.encode(scriptEachEntityIsAValueInAHash),
                            List.of(SafeEncoder.encode(parentKey), SafeEncoder.encode(id)),
                            List.of(configuration.binarySerialize(oldValue),
                                    configuration.binarySerialize(newValue))));
                } else {
                    runClusterCommand(parentKey, jedis -> jedis.eval(scriptEachEntityIsAValueInAHash,
                            List.of(parentKey, id),
                            List.of(configuration.serialize(oldValue),
                                    configuration.serialize(newValue))));
                }
                break;
            case EACH_ENTITY_IS_A_HASH:
            default:
                throw new UnsupportedOperationException("Unsupported RedisRepositoryStrategy!");
        }
    }

    /**
     * Update the entity identified by the given identifier to the new provided value if its old value is not equal with the given one.<br/>
     * This method is using a Lua script to do this in a transactional manner. <br/>
     * If the entity is not there, this method does nothing.<br/>
     * Warning: The EACH_ENTITY_IS_A_HASH strategy is not supported.<br/>
     * Implementation details: <br/>
     * - If the strategy used is EACH_ENTITY_IS_A_VALUE, the script calls the EXIST, GET and SET Redis commands.<br/>
     * - If the strategy used is EACH_ENTITY_IS_A_HASH, this method throws an unsupported operation exception.<br/>
     * - If the strategy used is EACH_ENTITY_IS_A_VALUE_IN_A_HASH, the script calls the HEXIST, HGET and HSET Redis commands.<br/>
     * @see <a href="https://redis.io/commands/EXIST">EXIST</a>
     * @see <a href="https://redis.io/commands/GET">GET</a>
     * @see <a href="https://redis.io/commands/SET">SET</a>
     * @see <a href="https://redis.io/commands/HEXIST">HEXIST</a>
     * @see <a href="https://redis.io/commands/HGET">HGET</a>
     * @see <a href="https://redis.io/commands/HSET">HSET</a>
     * @param id The String identifier of the entity
     * @param oldValue The old value of the entity
     * @param newValue The new value of the entity
     */
    @Override
    public final void updateIfItIsNot(final String id, final T oldValue, final T newValue) {
        throwIfNullOrEmptyOrBlank(id, "id");
        throwIfNull(oldValue, "oldValue");
        throwIfNull(newValue, "newValue");
        switch (strategy) {
            case EACH_ENTITY_IS_A_VALUE:
                final var scriptEachEntityIsAValue = LUA_SCRIPTS_UPDATE_IF_IT_IS_NOT.get(EACH_ENTITY_IS_A_VALUE);
                final var key = keyPrefix + id;
                if (configuration.useBinaryApi()) {
                    runClusterCommand(key, jedis -> jedis.eval(SafeEncoder.encode(scriptEachEntityIsAValue),
                            List.of(SafeEncoder.encode(key)),
                            List.of(configuration.binarySerialize(oldValue),
                                    configuration.binarySerialize(newValue))));
                } else {
                    runClusterCommand(key, jedis -> jedis.eval(scriptEachEntityIsAValue,
                            List.of(key),
                            List.of(configuration.serialize(oldValue),
                                    configuration.serialize(newValue))));
                }
                break;
            case EACH_ENTITY_IS_A_VALUE_IN_A_HASH:
                final var scriptEachEntityIsAValueInAHash = LUA_SCRIPTS_UPDATE_IF_IT_IS_NOT.get(EACH_ENTITY_IS_A_VALUE_IN_A_HASH);
                if (configuration.useBinaryApi()) {
                    runClusterCommand(parentKey, jedis -> jedis.eval(SafeEncoder.encode(scriptEachEntityIsAValueInAHash),
                            List.of(SafeEncoder.encode(parentKey), SafeEncoder.encode(id)),
                            List.of(configuration.binarySerialize(oldValue),
                                    configuration.binarySerialize(newValue))));
                } else {
                    runClusterCommand(parentKey, jedis -> jedis.eval(scriptEachEntityIsAValueInAHash,
                            List.of(parentKey, id),
                            List.of(configuration.serialize(oldValue),
                                    configuration.serialize(newValue))));
                }
                break;
            case EACH_ENTITY_IS_A_HASH:
            default:
                throw new UnsupportedOperationException("Unsupported RedisRepositoryStrategy!");
        }
    }

    /**
     * Delete the entity identified by the given identifier if its old value is equal with the given one.<br/>
     * This method is using a Lua script to do this in a transactional manner. <br/>
     * If the entity is not there, this method does nothing.<br/>
     * Warning: The EACH_ENTITY_IS_A_HASH strategy is not supported.<br/>
     * Implementation details: <br/>
     * - If the strategy used is EACH_ENTITY_IS_A_VALUE, the script calls the EXIST, GET and DEL Redis commands.<br/>
     * - If the strategy used is EACH_ENTITY_IS_A_HASH, this method throws an unsupported operation exception.<br/>
     * - If the strategy used is EACH_ENTITY_IS_A_VALUE_IN_A_HASH, the script calls the HEXIST, HGET and HDEL Redis commands.<br/>
     * @see <a href="https://redis.io/commands/EXIST">EXIST</a>
     * @see <a href="https://redis.io/commands/GET">GET</a>
     * @see <a href="https://redis.io/commands/DEL">DEL</a>
     * @see <a href="https://redis.io/commands/HEXIST">HEXIST</a>
     * @see <a href="https://redis.io/commands/HGET">HGET</a>
     * @see <a href="https://redis.io/commands/HDEL">HDEL</a>
     * @param id The String identifier of the entity
     * @param oldValue The old value of the entity
     */
    @Override
    public final void deleteIfItIs(final String id, final T oldValue) {
        throwIfNullOrEmptyOrBlank(id, "id");
        throwIfNull(oldValue, "oldValue");
        switch (strategy) {
            case EACH_ENTITY_IS_A_VALUE:
                final var scriptEachEntityIsAValue = LUA_SCRIPTS_DELETE_IF_IT_IS.get(EACH_ENTITY_IS_A_VALUE);
                final var key = keyPrefix + id;
                if (configuration.useBinaryApi()) {
                    runClusterCommand(key, jedis -> jedis.eval(SafeEncoder.encode(scriptEachEntityIsAValue),
                            List.of(SafeEncoder.encode(key)),
                            List.of(configuration.binarySerialize(oldValue))));
                } else {
                    runClusterCommand(key, jedis -> jedis.eval(scriptEachEntityIsAValue,
                            List.of(keyPrefix + id),
                            List.of(configuration.serialize(oldValue))));
                }
                break;
            case EACH_ENTITY_IS_A_VALUE_IN_A_HASH:
                final var scriptEachEntityIsAValueInAHash = LUA_SCRIPTS_DELETE_IF_IT_IS.get(EACH_ENTITY_IS_A_VALUE_IN_A_HASH);
                if (configuration.useBinaryApi()) {
                    runClusterCommand(parentKey, jedis -> jedis.eval(SafeEncoder.encode(scriptEachEntityIsAValueInAHash),
                            List.of(SafeEncoder.encode(parentKey), SafeEncoder.encode(id)),
                            List.of(configuration.binarySerialize(oldValue))));
                } else {
                    runClusterCommand(parentKey, jedis -> jedis.eval(scriptEachEntityIsAValueInAHash,
                            List.of(parentKey, id),
                            List.of(configuration.serialize(oldValue))));
                }
                break;
            case EACH_ENTITY_IS_A_HASH:
            default:
                throw new UnsupportedOperationException("Unsupported RedisRepositoryStrategy!");
        }
    }

    /**
     * Delete the entity identified by the given identifier if its old value is not equal with the given one.<br/>
     * This method is using a Lua script to do this in a transactional manner.<br/>
     * If the entity is not there, this method does nothing.<br/>
     * Warning: The EACH_ENTITY_IS_A_HASH strategy is not supported.<br/>
     * Implementation details: <br/>
     * - If the strategy used is EACH_ENTITY_IS_A_VALUE, the script calls the EXIST, GET and DEL Redis commands.<br/>
     * - If the strategy used is EACH_ENTITY_IS_A_HASH, this method throws an unsupported operation exception.<br/>
     * - If the strategy used is EACH_ENTITY_IS_A_VALUE_IN_A_HASH, the script calls the HEXIST, HGET and HDEL Redis commands.<br/>
     * @see <a href="https://redis.io/commands/EXIST">EXIST</a>
     * @see <a href="https://redis.io/commands/GET">GET</a>
     * @see <a href="https://redis.io/commands/DEL">DEL</a>
     * @see <a href="https://redis.io/commands/HEXIST">HEXIST</a>
     * @see <a href="https://redis.io/commands/HGET">HGET</a>
     * @see <a href="https://redis.io/commands/HDEL">HDEL</a>
     * @param id The String identifier of the entity
     * @param oldValue The old value of the entity
     */
    @Override
    public final void deleteIfItIsNot(final String id, final T oldValue) {
        throwIfNullOrEmptyOrBlank(id, "id");
        throwIfNull(oldValue, "oldValue");
        switch (strategy) {
            case EACH_ENTITY_IS_A_VALUE:
                final var scriptEachEntityIsAValue = LUA_SCRIPTS_DELETE_IF_IT_IS_NOT.get(EACH_ENTITY_IS_A_VALUE);
                final var key = keyPrefix + id;
                if (configuration.useBinaryApi()) {
                    runClusterCommand(key, jedis -> jedis.eval(SafeEncoder.encode(scriptEachEntityIsAValue),
                            List.of(SafeEncoder.encode(key)),
                            List.of(configuration.binarySerialize(oldValue))));

                } else {
                    runClusterCommand(key, jedis -> jedis.eval(scriptEachEntityIsAValue,
                            List.of(key),
                            List.of(configuration.serialize(oldValue))));
                }
                break;
            case EACH_ENTITY_IS_A_VALUE_IN_A_HASH:
                final var scriptEachEntityIsAValueInAHash = LUA_SCRIPTS_DELETE_IF_IT_IS_NOT.get(EACH_ENTITY_IS_A_VALUE_IN_A_HASH);
                if (configuration.useBinaryApi()) {
                    runClusterCommand(parentKey, jedis -> jedis.evalsha(SafeEncoder.encode(scriptEachEntityIsAValueInAHash),
                            List.of(SafeEncoder.encode(parentKey), SafeEncoder.encode(id)),
                            List.of(configuration.binarySerialize(oldValue))));
                } else {
                    runClusterCommand(parentKey, jedis -> jedis.evalsha(scriptEachEntityIsAValueInAHash,
                            List.of(parentKey, id),
                            List.of(configuration.serialize(oldValue))));
                }
                break;
            case EACH_ENTITY_IS_A_HASH:
            default:
                throw new UnsupportedOperationException("Unsupported RedisRepositoryStrategy!");
        }
    }

    private <K> K runClusterCommand(final String key, final Function<Jedis, K> operation) {
        throwIfNullOrEmptyOrBlank(key, "key");
        throwIfNull(operation, "operation");
        return new JedisClusterCommand<K>(jedisSlotBasedConnectionHandler, maxAttempts) {
            @Override
            public K execute(final Jedis connection) {
                try {
                    return operation.apply(connection);
                } catch (final JedisException exception) {
                    if (jedisExceptionInterceptor != null) {
                        jedisExceptionInterceptor.accept(exception);
                    }
                    throw exception;
                }
            }
        }.run(key);
    }

    private <K> K runClusterBinaryCommand(final byte[] key, final Function<Jedis, K> operation) {
        throwIfNullOrEmpty(key, "key");
        throwIfNull(operation, "operation");
        return new JedisClusterCommand<K>(jedisSlotBasedConnectionHandler, maxAttempts) {
            @Override
            public K execute(final Jedis connection) {
                try {
                    return operation.apply(connection);
                } catch (final JedisException exception) {
                    if (jedisExceptionInterceptor != null) {
                        jedisExceptionInterceptor.accept(exception);
                    }
                    throw exception;
                }
            }
        }.runBinary(key);
    }

    private Map<Integer, Set<String>> getSlots(final Set<String> keys) {
        return keys.stream()
                .collect(Collectors.groupingBy(JedisClusterCRC16::getSlot))
                .entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> Set.of(entry.getValue().toArray(String[]::new))));
    }

    private <K> K runClusterCommand(final Set<String> keys, final Function<Jedis, K> operation) {
        throwIfNullOrEmpty(keys, "keys");
        throwIfNull(operation, "operation");
        return new JedisClusterCommand<K>(jedisSlotBasedConnectionHandler, maxAttempts) {
            @Override
            public K execute(final Jedis connection) {
                try {
                    return operation.apply(connection);
                } catch (final JedisException exception) {
                    if (jedisExceptionInterceptor != null) {
                        jedisExceptionInterceptor.accept(exception);
                    }
                    throw exception;
                }
            }
        }.run(keys.size(), keys.toArray(String[]::new));
    }

    private <K> K runClusterBinaryCommand(final Set<byte[]> keys, final Function<Jedis, K> operation) {
        throwIfNullOrEmpty(keys, "keys");
        throwIfNull(operation, "operation");
        return new JedisClusterCommand<K>(jedisSlotBasedConnectionHandler, maxAttempts) {
            @Override
            public K execute(final Jedis connection) {
                try {
                    return operation.apply(connection);
                } catch (final JedisException exception) {
                    if (jedisExceptionInterceptor != null) {
                        jedisExceptionInterceptor.accept(exception);
                    }
                    throw exception;
                }
            }
        }.runBinary(keys.size(), keys.toArray(byte[][]::new));
    }

    private static final Map<RedisRepositoryStrategy, String> LUA_SCRIPTS_UPDATE_IF_IT_IS = Map.ofEntries(
            Map.entry(EACH_ENTITY_IS_A_VALUE,
                    "if redis.call('exists', KEYS[1]) == 1 and redis.call('get', KEYS[1]) == ARGV[1] then " +
                            "return redis.call('set', KEYS[1], ARGV[2]); " +
                            "end; " +
                            "return 0;"),
            Map.entry(EACH_ENTITY_IS_A_VALUE_IN_A_HASH,
                    "if redis.call('hexists', KEYS[1], KEYS[2]) == 1 and redis.call('hget', KEYS[1], KEYS[2]) == ARGV[1] then " +
                            "return redis.call('hset', KEYS[1], KEYS[2], ARGV[2]); " +
                            "end; " +
                            "return 0;")
    );

    private static final Map<RedisRepositoryStrategy, String> LUA_SCRIPTS_UPDATE_IF_IT_IS_NOT = Map.ofEntries(
            Map.entry(EACH_ENTITY_IS_A_VALUE,
                    "if redis.call('exists', KEYS[1]) == 1 and redis.call('get', KEYS[1]) ~= ARGV[1] then " +
                            "return redis.call('set', KEYS[1], ARGV[2]); " +
                            "end; " +
                            "return 0;"),
            Map.entry(EACH_ENTITY_IS_A_VALUE_IN_A_HASH,
                    "if redis.call('hexists', KEYS[1], KEYS[2]) == 1 and redis.call('hget', KEYS[1], KEYS[2]) ~= ARGV[1] then " +
                            "return redis.call('hset', KEYS[1], KEYS[2], ARGV[2]); " +
                            "end; " +
                            "return 0;")
    );

    private static final Map<RedisRepositoryStrategy, String> LUA_SCRIPTS_DELETE_IF_IT_IS = Map.ofEntries(
            Map.entry(EACH_ENTITY_IS_A_VALUE,
                    "if redis.call('exists', KEYS[1]) == 1 and redis.call('get', KEYS[1]) == ARGV[1] then " +
                            "return redis.call('del', KEYS[1]); " +
                            "end; " +
                            "return 0;"),
            Map.entry(EACH_ENTITY_IS_A_VALUE_IN_A_HASH,
                    "if redis.call('hexists', KEYS[1], KEYS[2]) == 1 and redis.call('hget', KEYS[1], KEYS[2]) == ARGV[1] then " +
                            "return redis.call('hdel', KEYS[1], KEYS[2]); " +
                            "end; " +
                            "return 0;")
    );

    private static final Map<RedisRepositoryStrategy, String> LUA_SCRIPTS_DELETE_IF_IT_IS_NOT = Map.ofEntries(
            Map.entry(EACH_ENTITY_IS_A_VALUE,
                    "if redis.call('exists', KEYS[1]) == 1 and redis.call('get', KEYS[1]) ~= ARGV[1] then " +
                            "return redis.call('del', KEYS[1]); " +
                            "end; " +
                            "return 0;"),
            Map.entry(EACH_ENTITY_IS_A_VALUE_IN_A_HASH,
                    "if redis.call('hexists', KEYS[1], KEYS[2]) == 1 and redis.call('hget', KEYS[1], KEYS[2]) ~= ARGV[1] then " +
                            "return redis.call('hdel', KEYS[1], KEYS[2]); " +
                            "end; " +
                            "return 0;")
    );

    private static <K> void throwIfNull(final K object, final String valueName) {
        if (object == null) {
            throw new IllegalArgumentException(valueName + " cannot be null!");
        }
    }

    private static void throwIfNullOrEmpty(final byte[] ids, final String valueName) {
        if (isNullOrEmpty(ids)) {
            throw new IllegalArgumentException(valueName + " cannot be null, nor empty!");
        }
    }

    private static <K> void throwIfNullOrEmpty(final Collection<K> ids, final String valueName) {
        if (isNullOrEmpty(ids)) {
            throw new IllegalArgumentException(valueName + " cannot be null, nor empty!");
        }
    }

    private static void throwIfNullOrEmptyOrBlank(final String id, final String valueName) {
        if (isNullOrEmptyOrBlank(id)) {
            throw new IllegalArgumentException(valueName + " cannot be null, nor empty!");
        }
    }

    private static <K> boolean isNullOrEmpty(final Collection<K> collection) {
        return collection == null || collection.isEmpty();
    }

    private static <K> boolean isNullOrEmpty(final Map<K, K> map) {
        return map == null || map.isEmpty();
    }

    private static boolean isNullOrEmptyOrBlank(final String text) {
        return text == null || text.isEmpty() || text.isBlank();
    }

    private static boolean isNullOrEmpty(final byte[] bytes) {
        return bytes == null || bytes.length == 0;
    }
}
