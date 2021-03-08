package com.github.mihaibogdaneugen.redisrepository;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.Map;
import java.util.Optional;

public abstract class StringHashRedisRepository<T extends RedisEntity<String>>
        extends BaseRedisRepository<String>
        implements StringRepository<T> {

    public StringHashRedisRepository(final Jedis jedis, final RedisRepositoryConfiguration configuration) {
        super(jedis, configuration);
    }

    public StringHashRedisRepository(final JedisPool jedisPool, final RedisRepositoryConfiguration configuration) {
        super(jedisPool, configuration);
    }

    public abstract Map<String, String> convertTo(final T entity);

    public abstract T convertFrom(final Map<String, String> entity);

    @Override
    public final String getKey(final String keySuffix) {
        return configuration.getKeyPrefix() + keySuffix;
    }

    @Override
    public final String getAllKeysPattern() {
        return configuration.getKeyPrefix() + "*";
    }

    @Override
    public final Optional<T> get(final String id) {
        throwIfNullOrEmptyOrBlank(id, "id");
        final var key = getKey(id);
        final var entity = jedis.hgetAll(key);
        return isNullOrEmpty(entity)
                ? Optional.empty()
                : Optional.of(convertFrom(entity));
    }

    @Override
    public final Boolean exists(final String id) {
        throwIfNullOrEmptyOrBlank(id, "id");
        final var key = getKey(id);
        return jedis.exists(key);
    }

    @Override
    public final void set(final T entity) {
        throwIfNull(entity, "entity");
        final var key = getKey(entity.getKey());
        jedis.hset(key, convertTo(entity));
    }
}
