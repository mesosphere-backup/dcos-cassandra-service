package com.mesosphere.dcos.cassandra.common.tasks;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.mesos.scheduler.ChainedObserver;
import org.apache.mesos.scheduler.plan.Phase;
import org.apache.mesos.state.JsonSerializer;
import org.apache.mesos.state.StateStore;
import org.apache.mesos.state.StateStoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mesosphere.dcos.cassandra.common.persistence.PersistenceException;

/**
 * Interface for managers of ClusterTask execution (e.g Backup, Restore, Cleanup, ... )
 *
 * @param <Context> the {@link ClusterTaskContext} used by the implementing manager
 */
public abstract class ClusterTaskManager<R extends ClusterTaskRequest, C extends ClusterTaskContext> extends ChainedObserver {
    /**
     * Use getClass() (NOT static class) to get/log implementing class.
     */
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private static final JsonSerializer SERIALIZER = new JsonSerializer();

    private final StateStore stateStore;
    private final String propertyKey;
    private final Class<C> clazz;

    private List<Phase> phases = Collections.emptyList();
    private volatile C activeContext = null; // used to signal that the operation has started

    protected ClusterTaskManager(StateStore stateStore, String propertyKey, Class<C> clazz) {
        this.stateStore = stateStore;
        this.propertyKey = propertyKey;
        this.clazz = clazz;
    }

    /**
     * Converts the provided request object to a context object.
     */
    protected abstract C toContext(R request);

    /**
     * Creates and returns a set of phases for this operation, given the provided context object.
     */
    protected abstract List<Phase> createPhases(C context);

    /**
     * Clears any entries related to this operation from the service state.
     */
    protected abstract void clearTasks() throws PersistenceException;

    /**
     * Restores previous state from state store. MUST be called by implementing classes at the end
     * of their constructor.
     */
    protected void restore() {
        try {
            C context = SERIALIZER.deserialize(stateStore.fetchProperty(propertyKey), clazz);
            if (context != null) {
                // Recovering from restart while operation was running
                phases = createPhases(context);
                for (Phase phase : phases) {
                    phase.subscribe(this);
                }
                this.activeContext = context;
            }
        } catch (IOException e) {
            logger.error("Error loading operation context from persistence store.", e);
        } catch (StateStoreException e) {
            logger.info("No operation context found.");
        }
    }

    public void start(R request) {
        if (isInProgress()) {
            logger.warn("Operation already in progress, context = {}", this.activeContext);
            return;
        }

        C context = toContext(request);
        logger.info("Starting operation");
        try {
            if (isComplete()) {
                clearTasks();
            }
            stateStore.storeProperty(propertyKey, SERIALIZER.serialize(context));
            phases = createPhases(context);
            for (Phase phase : phases) {
                phase.subscribe(this);
            }
            activeContext = context;
        } catch (IOException e) {
            logger.error("Error storing operation context into persistence store", e);
        }

        notifyObservers();
    }

    public void stop() {
        logger.info("Stopping operation");
        stateStore.clearProperty(propertyKey);
        try {
            clearTasks();
        } catch (PersistenceException e) {
            logger.error("Error deleting operation context from persistence store", e);
        }
        activeContext = null;
        phases = Collections.emptyList();
        notifyObservers();
    }

    public boolean isInProgress() {
        return activeContext != null && !isComplete();
    }

    public boolean isComplete() {
        if (activeContext == null) {
            return false;
        }
        for (Phase phase : phases) {
            if (!phase.isComplete()) {
                return false;
            }
        }
        return true;
    }

    public List<Phase> getPhases() {
        return phases;
    }
}
