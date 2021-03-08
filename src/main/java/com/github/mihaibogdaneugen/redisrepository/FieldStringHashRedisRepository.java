package com.github.mihaibogdaneugen.redisrepository;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.Optional;

public abstract class FieldStringHashRedisRepository<T extends RedisEntity<String>>
        extends BaseRedisRepository<String>
        implements StringRepository<T> {

    public FieldStringHashRedisRepository(final Jedis jedis, final RedisRepositoryConfiguration configuration) {
        super(jedis, configuration);
    }

    public FieldStringHashRedisRepository(final JedisPool jedisPool, final RedisRepositoryConfiguration configuration) {
        super(jedisPool, configuration);
    }

    public abstract String convertTo(final T entity);

    public abstract T convertFrom(final String entity);

    @Override
    public final Optional<T> get(final String id) {
        throwIfNullOrEmptyOrBlank(id, "id");
        final var entity = jedis.hget(configuration.getKeyPrefix(), id);
        return isNullOrEmptyOrBlank(entity)
                ? Optional.empty()
                : Optional.of(convertFrom(entity));
    }

    @Override
    public final Boolean exists(final String id) {
        throwIfNullOrEmptyOrBlank(id, "id");
        return jedis.hexists(configuration.getKeyPrefix(), id);
    }

    @Override
    public final void set(final T entity) {
        throwIfNull(entity, "entity");
        jedis.hset(configuration.getKeyPrefix(), entity.getKey(), convertTo(entity));
    }
}
