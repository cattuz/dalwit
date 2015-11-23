package com.devexed.dalwit.util;

import com.devexed.dalwit.DatabaseException;

import java.util.HashSet;
import java.util.Set;

public class CloseableManager<R extends Closeable> extends AbstractCloseable {

    protected final String managerName;
    protected final String resourceName;
    protected final Set<R> resources;

    public CloseableManager(Class<?> managerClass, Class<?> resourceClass, Set<R> resources) {
        this.managerName = managerClass.getSimpleName();
        this.resourceName = resourceClass.getSimpleName();
        this.resources = resources;
    }

    public CloseableManager(Class<?> managerClass, Class<?> resourceClass) {
        this(managerClass, resourceClass, new HashSet<R>());
    }

    public <E extends R> E open(E resource) {
        resources.add(resource);
        return resource;
    }

    @SuppressWarnings("unchecked")
    public void close(Object resource) {
        if (resource == null) return;

        R closeableResource = (R) resource;

        if (!resources.remove(closeableResource))
            throw new DatabaseException(resourceName + " not open in this " + managerName);

        closeableResource.close();
    }

    /**
     * Close all open resources.
     */
    @Override
    public void close() {
        for (R resource : resources) resource.close();
        super.close();
    }

}
