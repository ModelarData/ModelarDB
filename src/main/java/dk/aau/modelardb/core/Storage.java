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
import dk.aau.modelardb.core.models.ModelFactory;
import dk.aau.modelardb.core.utility.Pair;
import dk.aau.modelardb.core.utility.ValueFunction;
import dk.aau.modelardb.core.utility.Static;

import java.util.*;
import java.util.Map.Entry;

public abstract class Storage {

    /** Public Methods **/
    abstract public void open(Dimensions dimensions);
    abstract public void initialize(TimeSeriesGroup[] timeSeriesGroups,
                                    HashMap<Integer, Pair<String, ValueFunction>[]> derivedTimeSeries,
                                    Dimensions dimensions, String[] modelNames);
    abstract public void storeSegmentGroups(SegmentGroup[] segments, int length);
    abstract public Iterator<SegmentGroup> getSegmentGroups();
    abstract public int getMaxSID();
    abstract public int getMaxGID();
    abstract public void close();

    /** Protected Methods **/
    protected HashMap<String, Integer> initializeCaches(String[] modelNames,
                                                        Dimensions dimensions,
                                                        HashMap<String, Integer> modelsInStorage,
                                                        HashMap<Integer, Object[]> sourcesInStorage,
                                                        HashMap<Integer, Pair<String, ValueFunction>[]> derivedTimeSeries) {

        //The Dimensions object is stored so the schema can be retrieved later
        this.dimensions = dimensions;

        //Computes the set of models that must be inserted for the system to
        // function, per definition the mid of the fallback model is one
        HashMap<String, Integer> modelsToBeInserted = new HashMap<>();
        List<String> modelsWithFallback = new ArrayList<>(Arrays.asList(modelNames));
        modelsWithFallback.add(0, "dk.aau.modelardb.core.models.UncompressedModel");
        int mid = modelsInStorage.values().stream().max(Integer::compare).orElse(0);
        for (String model : modelsWithFallback) {
            if ( ! modelsInStorage.containsKey(model)) {
                mid += 1;
                modelsInStorage.put(model, mid);
                modelsToBeInserted.put(model, mid);
            }
        }

        //Verifies that the implementation of all models are loaded and creates the midCache and modelCache
        this.modelCache = new Model[modelsInStorage.size() + 1];
        this.midCache = new HashMap<>();
        for (Entry<String, Integer> entry : modelsInStorage.entrySet()) {
            Model model = ModelFactory.getModel(entry.getKey(), 0,0.0F, 0);
            mid = entry.getValue();
            this.modelCache[mid] = model;
            this.midCache.put(model.getClass().getName(), mid);
        }

        //Creates the sourceGroupCache, derivedSourceCache, the dimensionsCache, and the inverseDimensionsCache
        int nextSID = getMaxSID() + 1;
        int totalNumberOfSources = nextSID + derivedTimeSeries.entrySet().stream().mapToInt(e -> e.getValue().length).sum();
        this.sourceGroupCache = new int[totalNumberOfSources];
        this.sourceScalingFactorCache = new float[totalNumberOfSources];
        this.dimensionsCache = new Object[totalNumberOfSources][];
        HashMap<Integer, ArrayList<Integer>> gsc = new HashMap<>();
        ValueFunction scalingTransformation = new ValueFunction();
        this.sourceTransformationCache = new ValueFunction[totalNumberOfSources];
        HashMap<Integer, ArrayList<Integer>> groupDerivedCacheBuilder = new HashMap<>();
        for (Entry<Integer, Object[]> entry : sourcesInStorage.entrySet()) {
            //Metadata is a mapping from Sid to Scaling, Resolution, Gid, and Dimensions
            Integer sid = entry.getKey();
            Object[] metadata = entry.getValue();

            //Creates mappings from sid -> gid (sgc), from sid -> scaling factor (sc), and from sid -> dimensions (dmc)
            int gid = (int) metadata[2];
            this.sourceGroupCache[sid] = gid;
            this.sourceScalingFactorCache[sid] = (float) metadata[0];
            if ( ! gsc.containsKey(gid)) {
                //A group consist of time series with equivalent SI so we just store it once
                ArrayList<Integer> metadataArray = new ArrayList<>();
                metadataArray.add((int) metadata[1]);
                gsc.put(gid, metadataArray);
            }
            gsc.get(gid).add(sid);

            int dim = 0;
            Object[] columns = new Object[metadata.length - 3];
            for (int i = 3; i < metadata.length; i++) {
                columns[dim] = metadata[i];
                dim++;
            }
            this.dimensionsCache[sid] = columns;
            this.sourceTransformationCache[sid] = scalingTransformation;

            //Creates mappings from gid -> pair of sids for original and derived (gdc), and from sid -> to transformation (tc)
            if (derivedTimeSeries.containsKey(sid)) {
                //All sources that are derived should transformed by their user-defined function
                Pair<String, ValueFunction>[] sourcesAndTransformations = derivedTimeSeries.get(sid);
                ArrayList<Integer> gdcb = groupDerivedCacheBuilder.computeIfAbsent(gid, g -> new ArrayList<>());
                for (Pair<String, ValueFunction> sat : sourcesAndTransformations) {
                    int dsid = nextSID++;
                    this.sourceGroupCache[dsid] = gid;
                    this.sourceScalingFactorCache[dsid] = 1.0F; //HACK: scaling is assumed to be part of the transformation
                    this.dimensionsCache[dsid] = dimensions.get(sat._1);
                    this.sourceTransformationCache[dsid] = sat._2;
                    gdcb.add(sid);
                    gdcb.add(dsid);
                }
            }
        }
        this.groupDerivedCache = new HashMap<>();
        groupDerivedCacheBuilder.forEach((k, v) -> this.groupDerivedCache.put(k, v.stream().mapToInt(i -> i).toArray()));

        //The inverseDimensionsCache is constructed from the dimensions cache
        String[] columns = this.dimensions.getColumns();
        HashMap<String, HashMap<Object, HashSet<Integer>>> outer = new HashMap<>();
        for (int i = 1; i < this.dimensionsCache.length; i++) {
            //If data with existing sids are ingested missing sids can occur and must be handled
            if (this.dimensionsCache[i] == null) {
                Static.warn(String.format("CORE: a time series with sid %d does not exist", i));
                continue;
            }

            for (int j = 0; j < columns.length; j++) {
                Object value = this.dimensionsCache[i][j];
                HashMap<Object, HashSet<Integer>> inner = outer.getOrDefault(columns[j], new HashMap<>());
                HashSet<Integer> sids = inner.getOrDefault(value, new HashSet<>());
                sids.add(this.sourceGroupCache[i]);
                inner.put(value, sids);
                outer.put(columns[j], inner);
            }
        }

        this.inverseDimensionsCache = new HashMap<>();
        for (Entry<String, HashMap<Object, HashSet<Integer>>> oes : outer.entrySet()) {
            HashMap<Object, Integer[]> innerAsArray = new HashMap<>();
            for (Entry<Object, HashSet<Integer>> ies : oes.getValue().entrySet()) {
                Integer[] inner = ies.getValue().toArray(new Integer[0]);
                Arrays.sort(inner); //Sorted to make it simpler to read when debugging
                innerAsArray.put(ies.getKey(), inner);
            }
            this.inverseDimensionsCache.put(oes.getKey(), innerAsArray);
        }

        //Finally the sorted groupMetadataCache is created and consists of resolution and sids
        this.groupMetadataCache = new int[gsc.size() + 1][];
        gsc.forEach((k,v) -> {
            this.groupMetadataCache[k] = v.stream().mapToInt(i -> i).toArray();
            Arrays.sort(this.groupMetadataCache[k], 1, this.groupMetadataCache[k].length);
        });
        return modelsToBeInserted;
    }

    /** Instance Variables **/
    public Dimensions dimensions;

    //Write Cache: Maps the name of a model to the corresponding mid used in the data store
    public HashMap<String, Integer> midCache;

    //Read Cache: Maps the mid of a model to an instance of the model so a segment can be constructed
    public Model[] modelCache;

    //Read Cache: Maps the sid of a source to the gid of the group that the source is a member of
    public int[] sourceGroupCache;

    //Read Cache: Maps the gid of a group to pairs of sids for time series with derived time series
    public HashMap<Integer, int[]> groupDerivedCache;

    //Read Cache: Maps the sid of a source to the scaling factor specified for that source
    public float[] sourceScalingFactorCache;

    //Read Cache: Maps the sid of a source to the scaling factor specified for that source
    public ValueFunction[] sourceTransformationCache;

    //Read Cache: Maps the sid of a source to the members provided for that data source
    public Object[][] dimensionsCache;

    //Read Cache: Maps the value of a column for a dimension to the gids with that member
    public HashMap<String, HashMap<Object, Integer[]>> inverseDimensionsCache;

    //Read Cache: Maps the gid of a group to the groups resolution and the sids that are part of that group
    public int[][] groupMetadataCache;
}
