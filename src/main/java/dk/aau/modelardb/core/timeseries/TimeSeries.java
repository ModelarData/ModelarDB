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
package dk.aau.modelardb.core.timeseries;

import dk.aau.modelardb.core.DataPoint;

import java.io.Serializable;
import java.util.Iterator;

public abstract class TimeSeries implements  Serializable, Iterator<DataPoint> {
    /** Public Methods **/
    public TimeSeries(String source, int sid, int resolution) {
        this.source = source;
        this.sid = sid;
        this.resolution = resolution;
    }
    abstract public void open();
    abstract public void close();

    /** Instance Variables **/
    public final String source;
    public final int sid;
    public final int resolution;
    public float scalingFactor;
}