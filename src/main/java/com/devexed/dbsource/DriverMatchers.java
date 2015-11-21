package com.devexed.dbsource;

import java.util.regex.Pattern;

/**
 * Helper class providing driver matchers
 */
public final class DriverMatchers {

    // Hidden constructor
    private DriverMatchers() {}

    /**
     * @return A driver matcher that matches any driver.
     */
    public static DriverMatcher forAny() {
        return new AnyDriverMatcher();
    }

    /**
     * @return A driver matcher that matches no driver.
     */
    public static DriverMatcher forNone() {
        return new NoneDriverMatcher();
    }

    /**
     * @param type The type of the driver to match.
     * @return A driver matcher that matches a driver of a specific type.
     */
    public static DriverMatcher forType(String type) {
        return new TypeDriverMatcher(type);
    }

    /**
     * @param type The type of the driver to match.
     * @param version The compiled version pattern of the driver version to match.
     * @return A driver matcher that matches a driver of a specific type and version pattern.
     */
    public static DriverMatcher forPatternVersion(String type, Pattern version) {
        return new PatternDriverMatcher(type, version);
    }

    /**
     * Create a driver matcher only applicable to a certain database type whose version string interpreted as a
     * series of point-separated numbers is, in order, larger or equal to each of the integers in the minimum
     * version parameter. E.g. a specified minimum version of {2, 5, 8} matches the version string "2.5.9" and "3.0"
     * but not "1.2" or "2.5.3".
     *
     * @param type The type of the driver to match.
     * @param version The minimum version of the driver to match.
     * @return A driver matcher that matches a driver of a specific type and minimum version.
     */
    public static DriverMatcher forMinimumVersion(String type, int[] version) {
        return new MinimumVersionDriverMatcher(type, version);
    }

    private static class AnyDriverMatcher implements DriverMatcher {
        @Override
        public boolean matches(Driver driver) {
            return true;
        }
    }

    private static class NoneDriverMatcher implements DriverMatcher {
        @Override
        public boolean matches(Driver driver) {
            return true;
        }
    }

    private static class TypeDriverMatcher implements DriverMatcher {

        final String type;

        private TypeDriverMatcher(String type) {
            this.type = type;
        }

        @Override
        public boolean matches(Driver driver) {
            return type.equals(driver.getType());
        }

    }

    private static class PatternDriverMatcher extends TypeDriverMatcher {

        final Pattern version;

        private PatternDriverMatcher(String type, Pattern version) {
            super(type);
            this.version = version;
        }

        @Override
        public boolean matches(Driver driver) {
            return super.matches(driver) && version.matcher(driver.getVersion()).find();

        }

    }

    private static class MinimumVersionDriverMatcher extends TypeDriverMatcher {

        final int[] version;

        private MinimumVersionDriverMatcher(String type, int[] version) {
            super(type);
            this.version = version;
        }

        @Override
        public boolean matches(Driver driver) {
            if (!super.matches(driver)) return false;

            int versionIndex = 0;

            for (String part : driver.getType().split("\\.")) {
                if (versionIndex >= version.length) break;

                try {
                    if (Integer.parseInt(part) < version[versionIndex]) return false;
                } catch (Exception e) {
                    return false;
                }
            }

            return true;
        }

    }

}
