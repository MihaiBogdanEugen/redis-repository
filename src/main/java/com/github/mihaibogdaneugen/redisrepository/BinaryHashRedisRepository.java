package com.github.mihaibogdaneugen.redisrepository;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.util.SafeEncoder;

import java.util.Map;
import java.util.Optional;

public abstract class BinaryHashRedisRepository<T extends RedisEntity<byte[]>>
        extends BaseRedisRepository<byte[]>
        implements BinaryRepository<T> {

    public BinaryHashRedisRepository(final Jedis jedis, final RedisRepositoryConfiguration configuration) {
        super(jedis, configuration);
    }

    public BinaryHashRedisRepository(final JedisPool jedisPool, final RedisRepositoryConfiguration configuration) {
        super(jedisPool, configuration);
    }

    public abstract Map<byte[], byte[]> convertTo(final T entity);

    public abstract T convertFrom(final Map<byte[], byte[]> entity);

    @Override
    public final byte[] getKey(byte[] keySuffix) {
        return SafeEncoder.encode(configuration.getKeyPrefix() + SafeEncoder.encode(keySuffix));
    }

    @Override
    public final byte[] getAllKeysPattern() {
        return SafeEncoder.encode(configuration.getKeyPrefix() + "*");
    }

    @Override
    public final Optional<T> get(final byte[] id) {
        throwIfNullOrEmpty(id, "id");
        final var key = getKey(id);
        final var entity = jedis.hgetAll(key);
        return isNullOrEmpty(entity)
                ? Optional.empty()
                : Optional.of(convertFrom(entity));
    }

    @Override
    public final Boolean exists(final byte[] id) {
        throwIfNullOrEmpty(id, "id");
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
