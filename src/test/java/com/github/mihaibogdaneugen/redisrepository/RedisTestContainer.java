package com.github.mihaibogdaneugen.redisrepository;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

abstract class RedisTestContainer {

    static final Logger logger = LoggerFactory.getLogger(RedisTestContainer.class);

    static final int REDIS_PORT = 6379;

    static final GenericContainer<?> REDIS_CONTAINER;

    static {
        REDIS_CONTAINER = new GenericContainer<>(DockerImageName.parse("redis:6"))
                .withExposedPorts(REDIS_PORT);
        REDIS_CONTAINER.start();
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    static void safeThreadSleep(final long millis) {
        try {
             Thread.sleep(millis);
        } catch (final InterruptedException e) {
            logger.error(e.getMessage(), e);
        }
    }
}
