package com.github.mihaibogdaneugen.redisrepository;

public interface RedisEntity<KeyType> {
    KeyType getKey();
}
