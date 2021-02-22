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

import dk.aau.modelardb.core.models.Model;
import dk.aau.modelardb.core.models.Segment;
import dk.aau.modelardb.core.utility.Static;

import java.nio.ByteBuffer;
import java.util.HashMap;

public class SegmentGroup {

    /** Constructors **/
    public SegmentGroup(int gid, long startTime, long endTime, int mid, byte[] parameters, byte[] offsets) {
        this.gid = gid;
        this.startTime = startTime;
        this.endTime = endTime;
        this.mid = mid;
        this.parameters = parameters;
        this.offsets = offsets;
    }

    /** Public Methods **/
    public String toString() {
        //The segments might not represent all time series in the time series group
        int[] os = Static.bytesToInts(this.offsets);
        StringBuilder sb = new StringBuilder();
        sb.append("Segment: [").append(this.gid).append(" | ").append(this.startTime).append(" | ")
                .append(this.endTime).append(" | ").append(this.mid);
        for (int o : os) {
            sb.append(" | ").append(o);
        }
        sb.append("]");
        return sb.toString();
    }

    public SegmentGroup[] explode(int[][] groupMetadataCache, HashMap<Integer, int[]> groupDerivedCache) {
        int[] gmc = groupMetadataCache[this.gid];
        int[] derivedTimeSeries = groupDerivedCache.getOrDefault(this.gid, SegmentGroup.defaultDerivedTimeSeries);
        int[] timeSeriesInAGap = Static.bytesToInts(this.offsets);
        int temporalOffset = 0;
        if (timeSeriesInAGap.length > 0 && timeSeriesInAGap[timeSeriesInAGap.length - 1] < 0) {
            //HACK: a temporal offset from START might be store at the end as a negative integer as sids are always positive
            temporalOffset = -1 * timeSeriesInAGap[timeSeriesInAGap.length - 1];
        }
        int storedGroupSize = gmc.length - 1 - timeSeriesInAGap.length; //Minus one because gmc store SI at index zero

        //Creates a segment for all stored time series in the group that are not currently in a gap
        SegmentGroup[] segments;
        int nextSegment = 0;
        if (timeSeriesInAGap.length == 0) {
            //If no gaps exist, segments will be constructed for all stored time series and derived time series in the group
            int storedAndDerivedGroupSize = storedGroupSize + (derivedTimeSeries.length / 2);
            segments = new SegmentGroup[storedAndDerivedGroupSize];
            for (int index = 1; index < gmc.length; index++) {
                int sid = gmc[index];
                //Offsets store the following: [0] Group Offset, [1] Group Size, [2] Temporal Offset
                byte[] offset = ByteBuffer.allocate(12).putInt(nextSegment + 1).putInt(storedGroupSize).putInt(temporalOffset).array();
                segments[nextSegment] = new SegmentGroup(sid, this.startTime, this.endTime, this.mid, this.parameters, offset);
                nextSegment++;
            }
        } else {
            //If gaps exist, segments will not be constructed for time series in a gap and for time series that derive from them
            int storedAndDerivedGroupSize = storedGroupSize;
            for (int index = 1; index < gmc.length; index++) {
                int sid = gmc[index];
                if (( ! Static.contains(sid, timeSeriesInAGap)) && Static.contains(sid, derivedTimeSeries)) {
                    storedAndDerivedGroupSize += 1;
                }
            }

            segments = new SegmentGroup[storedAndDerivedGroupSize];
            for (int index = 1; index < gmc.length; index++) {
                int sid = gmc[index];
                if ( ! Static.contains(sid, timeSeriesInAGap)) {
                    //Offsets store the following: [0] Group Offset, [1] Group Size, [2] Temporal Offset
                    byte[] offset = ByteBuffer.allocate(12).putInt(nextSegment + 1).putInt(storedGroupSize).putInt(temporalOffset).array();
                    segments[nextSegment] = new SegmentGroup(sid, this.startTime, this.endTime, this.mid, this.parameters, offset);
                    nextSegment++;
                }
            }
        }

        //The segment for a derived time series are the same as the segment of their source time series, only the sid is changed
        for (int i = 0, j = 0; i < derivedTimeSeries.length && j < segments.length;) {
            if (derivedTimeSeries[i] == segments[j].gid) {
                segments[nextSegment] = new SegmentGroup(derivedTimeSeries[i + 1], this.startTime, this.endTime,
                        this.mid, this.parameters, segments[j].offsets);
                nextSegment++;
                i += 2;
            } else {
                j++;
            }
        }
        return segments;
    }

    public Segment[] toSegments(Storage storage) {
        int[][] groupMetadataCache = storage.getGroupMetadataCache();
        SegmentGroup[] sgs = this.explode(groupMetadataCache, storage.getGroupDerivedCache());
        Segment[] segments = new Segment[sgs.length];

        Model m = storage.getModelCache()[mid];
        int[] gmc = groupMetadataCache[this.gid];
        for (int i = 0; i < sgs.length; i++) {
            segments[i] = m.get(sgs[i].gid, this.startTime, this.endTime, gmc[0], this.parameters, sgs[i].offsets);
        }
        return segments;
    }

    /** Instance Variables **/
    public final int gid;
    public final long startTime;
    public final long endTime;
    public final int mid;
    public final byte[] parameters;
    public final byte[] offsets;
    private final static int[] defaultDerivedTimeSeries = new int[0];
}
