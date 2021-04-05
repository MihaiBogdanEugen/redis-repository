package com.github.mihaibogdaneugen.redisrepository;

public enum RedisRepositoryStrategy {
    NONE,
    EACH_ENTITY_IS_A_VALUE,
    EACH_ENTITY_IS_A_HASH,
    EACH_ENTITY_IS_A_VALUE_IN_A_HASH
}
