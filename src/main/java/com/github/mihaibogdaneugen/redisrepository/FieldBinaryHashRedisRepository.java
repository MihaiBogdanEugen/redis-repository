package com.github.mihaibogdaneugen.redisrepository;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.util.SafeEncoder;

import java.util.Optional;

public abstract class FieldBinaryHashRedisRepository<T extends RedisEntity<byte[]>>
        extends BaseRedisRepository<byte[]>
        implements BinaryRepository<T> {

    public FieldBinaryHashRedisRepository(final Jedis jedis, final RedisRepositoryConfiguration configuration) {
        super(jedis, configuration);
    }

    public FieldBinaryHashRedisRepository(final JedisPool jedisPool, final RedisRepositoryConfiguration configuration) {
        super(jedisPool, configuration);
    }

    public abstract byte[] convertTo(final T entity);

    public abstract T convertFrom(final byte[] entity);

    @Override
    public final Optional<T> get(final byte[] id) {
        throwIfNullOrEmpty(id, "id");
        final var entity = jedis.hget(getKey(), id);
        return isNullOrEmpty(entity)
                ? Optional.empty()
                : Optional.of(convertFrom(entity));
    }

    @Override
    public final Boolean exists(final byte[] id) {
        throwIfNullOrEmpty(id, "id");
        return jedis.hexists(getKey(), id);
    }

    @Override
    public final void set(final T entity) {
        throwIfNull(entity, "entity");
        jedis.hset(getKey(), entity.getKey(), convertTo(entity));
    }

    private byte[] getKey() {
        return SafeEncoder.encode(configuration.getKeyPrefix());
    }
}
