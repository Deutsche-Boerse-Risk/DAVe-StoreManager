package com.deutscheboerse.risk.dave.healthcheck;

import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.shareddata.LocalMap;

/**
 * Holds a global state of all verticles which are vital for proper
 * run of DAVE-MarginLoader.
 * <p>
 * If all components are up and running the {@link HealthCheck#ready()}
 * method returns {@code true}.
 */
public class HealthCheck {
    private static final Logger LOG = LoggerFactory.getLogger(HealthCheck.class);

    private static final String MAP_NAME = "healthCheck";

    public enum Component {
        API,
        PERSISTENCE_SERVICE
    }

    private LocalMap<String, Boolean> localMap;

    /**
     * Create a new instance.
     *
     * @param vertx All instances of {@code HealthCheck} created with identical
     *              {@code vertx} parameter share the same local map.
     */
    public HealthCheck(Vertx vertx) {
        LOG.trace("Constructing {} object", HealthCheck.class.getCanonicalName());
        localMap = vertx.sharedData().getLocalMap(MAP_NAME);
        for (Component component: Component.values()) {
            localMap.putIfAbsent(component.name(), false);
        }
    }

    /**
     * Indicates whether all verticles are running properly.
     *
     * @return {@code true} if all verticles are up and running.
     */
    public boolean ready() {
        LOG.trace("Received readiness query");
        // Return true only if all the values are true (the map
        // does not contain any single false)
        return !localMap.values().contains(false);
    }

    public HealthCheck setComponentReady(Component component) {
        LOG.info("Setting {} readiness to {}", component.name(), true);
        localMap.put(component.name(), true);
        return this;
    }

    public HealthCheck setComponentFailed(Component component) {
        LOG.info("Setting {} readiness to {}", component.name(), false);
        localMap.put(component.name(), false);
        return this;
    }

    public boolean isComponentReady(Component component) {
        return localMap.get(component.name());
    }
}
