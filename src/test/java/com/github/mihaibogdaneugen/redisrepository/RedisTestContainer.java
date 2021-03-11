package com.github.mihaibogdaneugen.redisrepository;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

abstract class RedisTestContainer {

    static final int REDIS_PORT = 6379;

    static final GenericContainer<?> REDIS_CONTAINER;

    static {
        REDIS_CONTAINER = new GenericContainer<>(DockerImageName.parse("redis:6"))
                .withExposedPorts(REDIS_PORT);
        REDIS_CONTAINER.start();
        REDIS_CONTAINER.close();
    }
}
