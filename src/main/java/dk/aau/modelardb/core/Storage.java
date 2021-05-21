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

import dk.aau.modelardb.core.models.ModelType;
import dk.aau.modelardb.core.models.ModelTypeFactory;
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
    abstract public int getMaxTid();
    abstract public int getMaxGid();
    abstract public void close();
    abstract public void storeSegmentGroups(SegmentGroup[] segments, int size);

    /** Protected Methods **/
    protected HashMap<String, Integer> initializeCaches(String[] modelNames,
                                                        Dimensions dimensions,
                                                        HashMap<String, Integer> modelsInStorage,
                                                        HashMap<Integer, Object[]> timeSeriesInStorage,
                                                        HashMap<Integer, Pair<String, ValueFunction>[]> derivedTimeSeries) {

        //The Dimensions object is stored so the schema can be retrieved later
        this.dimensions = dimensions;

        //Computes the set of models that must be inserted for the system to
        // function, per definition the mtid of the fallback model is one
        HashMap<String, Integer> modelsToBeInserted = new HashMap<>();
        List<String> modelsWithFallback = new ArrayList<>(Arrays.asList(modelNames));
        modelsWithFallback.add(0, "dk.aau.modelardb.core.models.UncompressedModelType");
        int mtid = modelsInStorage.values().stream().max(Integer::compare).orElse(0);
        for (String model : modelsWithFallback) {
            if ( ! modelsInStorage.containsKey(model)) {
                mtid += 1;
                modelsInStorage.put(model, mtid);
                modelsToBeInserted.put(model, mtid);
            }
        }

        //Verifies that the implementation of all models are loaded and creates the mtidCache and modelCache
        this.modelTypeCache = new ModelType[modelsInStorage.size() + 1];
        this.mtidCache = new HashMap<>();
        for (Entry<String, Integer> entry : modelsInStorage.entrySet()) {
            ModelType modelType = ModelTypeFactory.getModel(entry.getKey(), 0,0.0F, 0);
            mtid = entry.getValue();
            this.modelTypeCache[mtid] = modelType;
            this.mtidCache.put(modelType.getClass().getName(), mtid);
        }

        //Creates the timeSeriesGroupCache, derivedSourceCache, the dimensionsCache, and the inverseDimensionsCache
        int nextTid = getMaxTid() + 1;
        int totalNumberOfSources = nextTid + derivedTimeSeries.entrySet().stream().mapToInt(e -> e.getValue().length).sum();
        this.timeSeriesGroupCache = new int[totalNumberOfSources];
        this.timeSeriesScalingFactorCache = new float[totalNumberOfSources];
        this.dimensionsCache = new Object[totalNumberOfSources][];
        HashMap<Integer, ArrayList<Integer>> gsc = new HashMap<>();
        ValueFunction scalingTransformation = new ValueFunction();
        this.timeSeriesTransformationCache = new ValueFunction[totalNumberOfSources];
        HashMap<Integer, ArrayList<Integer>> groupDerivedCacheBuilder = new HashMap<>();
        for (Entry<Integer, Object[]> entry : timeSeriesInStorage.entrySet()) {
            //Metadata is a mapping from Tid to Scaling, Sampling Interval, Gid, and Dimensions
            Integer tid = entry.getKey();
            Object[] metadata = entry.getValue();

            //Creates mappings from tid -> gid (tsgc), from tid -> scaling factor (sc), and from tid -> dimensions (dmc)
            int gid = (int) metadata[2];
            this.timeSeriesGroupCache[tid] = gid;
            this.timeSeriesScalingFactorCache[tid] = (float) metadata[0];
            if ( ! gsc.containsKey(gid)) {
                //A group consist of time series with equivalent SI so we just store it once
                ArrayList<Integer> metadataArray = new ArrayList<>();
                metadataArray.add((int) metadata[1]);
                gsc.put(gid, metadataArray);
            }
            gsc.get(gid).add(tid);

            int dim = 0;
            Object[] columns = new Object[metadata.length - 3];
            for (int i = 3; i < metadata.length; i++) {
                columns[dim] = metadata[i];
                dim++;
            }
            this.dimensionsCache[tid] = columns;
            this.timeSeriesTransformationCache[tid] = scalingTransformation;

            //Creates mappings from gid -> pair of tids for original and derived (gdc), and from tid -> to transformation (tc)
            if (derivedTimeSeries.containsKey(tid)) {
                //All derived time series perform all of their transformations in their user-defined function
                Pair<String, ValueFunction>[] sourcesAndTransformations = derivedTimeSeries.get(tid);
                ArrayList<Integer> gdcb = groupDerivedCacheBuilder.computeIfAbsent(gid, g -> new ArrayList<>());
                for (Pair<String, ValueFunction> sat : sourcesAndTransformations) {
                    int dtid = nextTid++;
                    this.timeSeriesGroupCache[dtid] = gid;
                    this.timeSeriesScalingFactorCache[dtid] = 1.0F; //HACK: scaling is assumed to be part of the transformation
                    this.dimensionsCache[dtid] = dimensions.get(sat._1);
                    this.timeSeriesTransformationCache[dtid] = sat._2;
                    gdcb.add(tid);
                    gdcb.add(dtid);
                }
            }
        }
        this.groupDerivedCache = new HashMap<>();
        groupDerivedCacheBuilder.forEach((k, v) -> this.groupDerivedCache.put(k, v.stream().mapToInt(i -> i).toArray()));

        //The inverseDimensionsCache is constructed from the dimensions cache
        String[] columns = this.dimensions.getColumns();
        HashMap<String, HashMap<Object, HashSet<Integer>>> outer = new HashMap<>();
        for (int i = 1; i < this.dimensionsCache.length; i++) {
            //If data with existing tids are ingested missing tids can occur and must be handled
            if (this.dimensionsCache[i] == null) {
                Static.warn(String.format("CORE: a time series with tid %d does not exist", i));
                continue;
            }

            for (int j = 0; j < columns.length; j++) {
                Object value = this.dimensionsCache[i][j];
                HashMap<Object, HashSet<Integer>> inner = outer.getOrDefault(columns[j], new HashMap<>());
                HashSet<Integer> tids = inner.getOrDefault(value, new HashSet<>());
                tids.add(this.timeSeriesGroupCache[i]);
                inner.put(value, tids);
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
            //Some engines converts all columns to uppercase so the caches key must also be so
            this.inverseDimensionsCache.put(oes.getKey().toUpperCase(), innerAsArray);
        }

        //Finally the sorted groupMetadataCache is created and consists of sampling interval and tids
        this.groupMetadataCache = new int[gsc.size() + 1][];
        gsc.forEach((k,v) -> {
            this.groupMetadataCache[k] = v.stream().mapToInt(i -> i).toArray();
            Arrays.sort(this.groupMetadataCache[k], 1, this.groupMetadataCache[k].length);
        });
        return modelsToBeInserted;
    }

    /** Instance Variables **/
    public Dimensions dimensions;

    //Write Cache: Maps the name of a model to the corresponding mtid used by the storage layer
    public HashMap<String, Integer> mtidCache;

    //Read Cache: Maps the mtid of a model to an instance of the model so segments can be constructed from it
    public ModelType[] modelTypeCache;

    //Read Cache: Maps the tid of a time series to the gid of the group that the time series is a member of
    public int[] timeSeriesGroupCache;

    //Read Cache: Maps the gid of a group to pairs of tids for time series with derived time series
    public HashMap<Integer, int[]> groupDerivedCache;

    //Read Cache: Maps the tid of a time series to the scaling factor specified for for the time series source
    public float[] timeSeriesScalingFactorCache;

    //Read Cache: Maps the tid of a time series to the transformation specified for the time series source
    public ValueFunction[] timeSeriesTransformationCache;

    //Read Cache: Maps the tid of a time series to the members specified for the time series source
    public Object[][] dimensionsCache;

    //Read Cache: Maps the value of a column for a dimension to the gids with that member
    public HashMap<String, HashMap<Object, Integer[]>> inverseDimensionsCache;

    //Read Cache: Maps the gid of a group to the groups sampling interval and the tids that are part of that group
    public int[][] groupMetadataCache;
}
