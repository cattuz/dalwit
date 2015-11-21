package com.devexed.dbsource;

/**
 * Matches database drivers.
 *
 * @see Driver
 */
public interface DriverMatcher {

    /**
     * @param driver The driver to match.
     * @return True if this matcher matches the driver.
     */
    boolean matches(Driver driver);

}
