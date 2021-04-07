/* Copyright 2018-2020 Aalborg University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dk.aau.modelardb.core;

import java.util.*;
import java.util.stream.Collectors;

public class Configuration {

    /** Constructors **/
    public Configuration() {
        this.values = new HashMap<>();
    }

    /** Public Methods **/
    public Object add(String name, Object value) {

        Object[] values;
        if (value instanceof String) {
            value = this.infer((String) value);
            this.validate(name, value);
            values = new Object[]{value};
        }  else if (value.getClass().isArray()) {
            values = (Object[]) value;
        } else {
            values = new Object[]{value};
        }
        this.values.merge(name, values, this::mergeArrays);
        return value;
    }

    public Object[] remove(String value) {
        return this.values.remove(value);
    }

    public boolean contains(String value) {
        return this.values.containsKey(value);
    }

    public void containsOrThrow(String... values) {
        //All of the missing values should be one error
        ArrayList<String> missingValues = new ArrayList<>();
        for (String value : values) {
            if ( ! this.values.containsKey(value)) {
                missingValues.add(value);
            }
        }

        //If values are missing execution cannot continue
        if ( ! missingValues.isEmpty()) {
            throw new IllegalArgumentException("ModelarDB: the following required options are not in the configuration file " +
                    missingValues.stream().collect(Collectors.joining(" ")));
        }
    }

    //Generic Getters
    public Object[] get(String name) {
        return this.values.get(name);
    }

    public String getString(String name) {
        return (String) getObject(name);
    }

    public boolean getBoolean(String name) {
        return (boolean) getObject(name);
    }

    public int getInteger(String name) {
        return (int) getObject(name);
    }

    public float getFloat(String name) {
        try {
            return (float) getObject(name);
        } catch (ClassCastException cce) {
            return (int) getObject(name);
        }
    }

    public String[] getArray(String name) {
        return Arrays.stream(this.values.get(name)).toArray(String[]::new);
    }

    //Specific Getters
    public float getError() {
        return getFloat("modelardb.error");
    }

    public int getLatency() {
        return getInteger("modelardb.latency");
    }

    public int getLimit() {
        return getInteger("modelardb.limit");
    }

    public int getResolution() {
        return getInteger("modelardb.resolution");
    }

    public String[] getModels() {
        return getArray("modelardb.model");
    }

    public String[] getDataSources() {
        return getArray("modelardb.source");
    }

    public Dimensions getDimensions() {
        return (Dimensions) getObject("modelardb.dimensions");
    }

    public String getStorage() {
        return getString("modelardb.storage");
    }

    public TimeZone getTimeZone() {
        if (values.containsKey("modelardb.timezone")) {
            return TimeZone.getTimeZone((String) values.get("modelardb.timezone")[0]);
        } else {
            return TimeZone.getDefault();
        }
    }

    /** Private Methods **/
    private Object infer(String value) {
        //Attempts to infer a more concrete type for the string
        if (value.equalsIgnoreCase("true")) {
            return true;
        } else if (value.equalsIgnoreCase("false")) {
            return false;
        }

        try {
            return Integer.parseInt(value);
        } catch (Exception ignored) {
        }

        try {
            return Float.parseFloat(value);
        } catch (Exception ignored) {
        }

        return value;
    }

    private void validate(String key, Object value) {
        //Settings used by the core are checked to ensure their values are within the expected range
        switch (key) {
            case "modelardb.batch":
                if ( ! (value instanceof Integer) || (int) value <= 0) {
                    throw new IllegalArgumentException("CORE: modelardb.batch must be a positive number");
                }
                break;
            case "modelardb.error":
                if (( ! (value instanceof Float) || (float) value < 0.0 || 100.0 < (float) value) &&
                        ( ! (value instanceof Integer) || (int) value < 0.0 || 100.0 < (int) value)) {
                    throw new IllegalArgumentException("CORE: modelardb.error must be a percentage from 0.0 to 100.0");
                }
                break;
            case "modelardb.latency":
                if ( ! (value instanceof Integer) || (int) value <= 0) {
                    throw new IllegalArgumentException("CORE: modelardb.latency must be a positive number of seconds");
                }
                break;
            case "modelardb.limit":
                if ( ! (value instanceof Integer) || (int) value <= 0) {
                    throw new IllegalArgumentException("CORE: modelardb.limit must be a positive number of data point groups");
                }
                break;
            case "modelardb.resolution":
                if ( ! (value instanceof Integer) || (int) value <= 0) {
                    throw new IllegalArgumentException("CORE: modelardb.resolution must be a positive number of seconds");
                }
                break;
            case "modelardb.ingestors":
                if ( ! (value instanceof Integer) || (int) value <= 0) {
                    throw new UnsupportedOperationException("ModelarDB: modelardb.ingestors must be a positive number of ingestors");
                }
                break;
            case "modelardb.timezone":
                if ( ! (value instanceof String) || ! TimeZone.getTimeZone((String) value).getID().equals(value)) {
                    throw new UnsupportedOperationException("ModelarDB: modelardb.timezone must be a valid time zone id");
                }
        }
    }

    private Object[] mergeArrays(Object[] a, Object[] b) {
        int j = 0;
        Object[] c = new Object[a.length + b.length];

        for (Object o : a) {
            c[j] = o;
            j++;
        }

        for (Object o : b) {
            c[j] = o;
            j++;
        }
        return c;
    }

    private Object getObject(String name) {
        Object[] values = this.values.get(name);
        if (values == null) {
            throw new UnsupportedOperationException("CORE: configuration \"" + name + "\" does not exist");
        }

        if (values.length > 1) {
            throw new UnsupportedOperationException("CORE: configuration \"" + name + "\" is not unique");
        }
        return values[0];
    }

    /** Instance Variables **/
    private final HashMap<String, Object[]> values;
}
