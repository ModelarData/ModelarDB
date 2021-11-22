/* Copyright 2021 The ModelarDB Contributors
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
package dk.aau.modelardb.storage

import dk.aau.modelardb.OffsetType
import dk.aau.modelardb.config.ModelarConfig
import dk.aau.modelardb.core.models.{ModelType, ModelTypeFactory}
import dk.aau.modelardb.core.utility.{Pair, Static, ValueFunction}
import dk.aau.modelardb.core.{Dimensions, TimeSeriesGroup}

import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable
import scala.math.Ordering.Implicits.infixOrderingOps

abstract class Storage(tidOffset: Int) {

  val tidCounter = new AtomicInteger(0)
  val gidCounter = new AtomicInteger(0)
  private val offsetCache = mutable.Map.empty[String, mutable.Map[OffsetType, Int]]

  protected def storeOffset(edgeId: String, offset: Int, offsetType: OffsetType): Unit
  protected def readOffset(edgeId: String, offsetType: OffsetType): Option[Int]
  protected def initializeOffsetCache(): Unit

  def updateOffsetCache(key: String, offset: Int, offsetType: OffsetType): Int = {
    offsetType match {
      case OffsetType.TID | OffsetType.GID =>
        if (offsetCache.contains(key)) {
          val innerMap = offsetCache(key)
          innerMap.put(offsetType, offset)
          offsetCache.put(key, innerMap)
          offset
        } else {
          offsetCache.put(key, mutable.Map[OffsetType, Int](offsetType -> offset))
          offset
        }
      case _ @ unknown => throw new Exception(s"Unknown OffsetType: $unknown")
    }
  }

  def getOffset(offsetType: OffsetType, edgeId: String, count: Int): Int = {
    val edgeIdInCache = offsetCache.contains(edgeId)
    val offsetInCache = offsetCache
      .get(edgeId)
      .flatMap(_.get(offsetType))

    (edgeIdInCache, offsetInCache) match {
      case (_, Some(offset)) => offset
      case (false, _) | (true, None) =>
        val offsetInDB = readOffset(edgeId, offsetType)
        offsetInDB match {
          case None =>
            val newOffset = offsetType match {
              case OffsetType.TID => tidCounter.getAndAdd(count)
              case OffsetType.GID => gidCounter.getAndAdd(count)
            }
            offsetCache.put(edgeId, mutable.Map((offsetType, newOffset)))
            storeOffset(edgeId, newOffset, offsetType)
            newOffset
          case Some(offset) => updateOffsetCache(edgeId, offset, offsetType)
//          case Some(offset) if edgeIdInCache =>
//            offsetCache(edgeId).put(offsetType, offset)
//            offset
//          case Some(offset) if !edgeIdInCache =>
//            offsetCache.put(edgeId, mutable.Map(offsetType -> offset ))
//            offset
          case _ => throw new IllegalStateException(s"Offset cache in illegal state")
        }
      case _ => throw new IllegalStateException(s"Offset cache in illegal state")
    }
  }

//  def getTidOffset(edgeId: String, count: Int): Int = {
//    val edgeIdInCache = offsetCache.contains(edgeId)
//    val tidInCache = offsetCache
//      .get(edgeId)
//      .flatMap(_.get(OffsetType.TID))
//
//    (edgeIdInCache, tidInCache) match {
//      case (_, Some(tidOffset)) => tidOffset
//      case (false, _) | (true, None) =>
//        val offsetInDB = readOffset(edgeId, OffsetType.TID)
//        offsetInDB match {
//          case None =>
//            val tidOffset = tidCounter.getAndAdd(count)
//            offsetCache.put(edgeId, mutable.Map((OffsetType.TID, tidOffset)))
//            storeOffset(edgeId, tidOffset, OffsetType.TID)
//            tidOffset
//          case Some(tidOffset) if edgeIdInCache =>
//            offsetCache(edgeId).put(OffsetType.TID, tidOffset)
//            tidOffset
//          case Some(tidOffset) if !edgeIdInCache =>
//            offsetCache.put(edgeId, mutable.Map(OffsetType.TID -> tidOffset ))
//            tidOffset
//          case _ => throw new IllegalStateException(s"Offset cache in illegal state")
//        }
//      case _ => throw new IllegalStateException(s"Offset cache in illegal state")
//    }
//  }
//
//  def getGidOffset(edgeId: String, count: Int): Int = {
//    val hasEdgeId = offsetCache.contains(edgeId)
//    val hasGid = offsetCache
//      .get(edgeId)
//      .flatMap(_.get(OffsetType.GID))
//
//    (hasEdgeId, hasGid) match {
//      case (false, _) | (true, None) =>
//        val gidOffset = gidCounter.getAndAdd(count)
//        offsetCache.put(edgeId, mutable.Map((OffsetType.GID, gidOffset)))
//        storeOffset(edgeId, gidOffset, OffsetType.GID)
//        gidOffset
//      case (_, Some(gid)) => gid
//    }
//  }

//  protected val tidCounter: AtomicInteger = new AtomicInteger(tidOffset)
//  protected val gidCounter: AtomicInteger = new AtomicInteger(0)
//
//  def getTidOffset(edgeId: String, count: Int): Int = {
//    val offset = tidCounter.addAndGet(count)
//    storeOffset(edgeId, offset, "TID")
//    offset
//  }
//
//
//  def getGidOffset(edgeId: String, count: Int): Int = {
//    val offset = gidCounter.addAndGet(count)
//    storeOffset(edgeId, offset, "GID")
//    offset
//  }

  /** Public Methods * */
  def open(dimensions: Dimensions): Unit
  def getMinTid: Int
  def getMaxTid: Int
  def getMinGid: Int
  def getMaxGid: Int
  def close(): Unit

  def storeMetadataAndInitializeCaches(config: ModelarConfig, timeSeriesGroups: Array[TimeSeriesGroup], gidOffset: Int): Unit = {
    gidCounter.compareAndSet(0, gidOffset)
    //The Dimensions object is stored so the schema can be retrieved later
    dimensions = config.dimensions

    //Inserts the metadata for the sources defined in the configuration file (Tid, Scaling Factor,
    // Sampling Interval, Gid, Dimensions) into the persistent storage defined by modelar.storage.
    storeTimeSeries(timeSeriesGroups, tidOffset)

    //Computes the set of model types that must be inserted for the system to
    // function, per definition the mtid of the fallback model type is one
    val modelTypesToBeInserted = mutable.HashMap[String, Integer]()
    val modelsWithFallback = mutable.ArrayBuffer[String](config.models: _*)
    modelsWithFallback.prepend("dk.aau.modelardb.core.models.UncompressedModelType")
    val modelTypesInStorage = this.getModelTypes
    var mtid: Integer = modelTypesInStorage.values.reduceOption(_ max _).getOrElse(0)

    for (model <- modelsWithFallback) {
      if ( ! modelTypesInStorage.contains(model)) {
        mtid += 1
        modelTypesInStorage.put(model, mtid)
        modelTypesToBeInserted.put(model, mtid)
      }
    }

    //Verifies that the implementation of all model types are loaded and creates the modelTypeCache and mtidCache
    this.modelTypeCache = Array.fill[ModelType](modelTypesInStorage.size + 1)(null)
    this.mtidCache = mutable.HashMap[String, Integer]()

    for ((modelTypeName, mtid) <- modelTypesInStorage) {
      val modelType = ModelTypeFactory.getModel(modelTypeName, 0, 0.0F, 0)
      this.modelTypeCache(mtid) = modelType
      this.mtidCache.put(modelType.getClass.getName, mtid)
    }

    //Creates the timeSeriesGroupCache, timeSeriesScalingFactorCache, and timeSeriesMembersCache
    var nextTid = getMaxTid + 1
    val derivedTimeSeries = config.derivedTimeSeries
    val totalNumberOfSources = nextTid + derivedTimeSeries.values.stream.mapToInt((v: Array[Pair[String, ValueFunction]]) => v.length).sum
    timeSeriesGroupCache = new ArrayCache(totalNumberOfSources, tidCounter.get())
    timeSeriesSamplingIntervalCache = new ArrayCache(totalNumberOfSources, tidCounter.get())
    timeSeriesScalingFactorCache = new ArrayCache(totalNumberOfSources, tidCounter.get())
    timeSeriesMembersCache = new ArrayCache[Array[AnyRef]](totalNumberOfSources, tidCounter.get())
    val gsc = mutable.HashMap[Integer, mutable.ArrayBuffer[Integer]]()
    val scalingTransformation = new ValueFunction()
    timeSeriesTransformationCache = new ArrayCache[ValueFunction](totalNumberOfSources, tidCounter.get())
    val groupDerivedCacheBuilder = mutable.HashMap[Int, mutable.ArrayBuffer[Integer]]()
    val timeSeriesInStorage = this.getTimeSeries
    for ((tid, metadata) <- timeSeriesInStorage) {
      //Metadata is a mapping from Tid to Scaling, Sampling Interval, Gid, and Dimensions
      val gid = metadata(2).asInstanceOf[Int]
      //Creates mappings from tid -> gid, tid -> sampling interval, tid -> scaling factor, and tid -> dimensions
      timeSeriesGroupCache.set(tid, gid)
      timeSeriesSamplingIntervalCache.set(tid, metadata(1).asInstanceOf[Int])
      timeSeriesScalingFactorCache.set(tid, metadata(0).asInstanceOf[Float])
      if ( ! gsc.contains(gid)) {
        //A group consist of time series with equivalent SI
        val metadataArray = mutable.ArrayBuffer[Integer]()
        metadataArray.append(metadata(1).asInstanceOf[Int])
        gsc.put(gid, metadataArray)
      }
      gsc(gid).append(tid)

      var dim = 0
      val columns = Array.fill[AnyRef](metadata.length - 3)(null)
      for (i <- 3 until metadata.length) {
        columns(dim) = metadata(i)
        dim += 1
      }
      timeSeriesMembersCache.set(tid, columns)
      timeSeriesTransformationCache.set(tid, scalingTransformation)

      //Creates mappings from gid -> pair of tids for original and derived (gdc), and from tid -> to transformation (tc)
      if (derivedTimeSeries.containsKey(tid)) {
        //All derived time series perform all of their transformations in their user-defined function
        val sourcesAndTransformations = derivedTimeSeries.get(tid)
        val gdcb: mutable.ArrayBuffer[Integer] = groupDerivedCacheBuilder.getOrElse(gid, mutable.ArrayBuffer[Integer]())
        for (sat <- sourcesAndTransformations) {
          val dtid = { nextTid += 1; nextTid - 1 }  //nextTid++
          timeSeriesGroupCache.set(dtid, gid)
          timeSeriesSamplingIntervalCache.set(dtid, metadata(1).asInstanceOf[Int])
          timeSeriesScalingFactorCache.set(dtid, 1.0F) //HACK: scaling is assumed to be part of the transformation
          timeSeriesMembersCache.set(dtid, dimensions.get(sat._1))
          timeSeriesTransformationCache.set(dtid, sat._2)
          gdcb.append(tid)
          gdcb.append(dtid)
        }
      }
    }

    groupDerivedCache = groupDerivedCacheBuilder
//      .map { case (key, value) => (key, value.map(i => i.intValue()).toArray)
      .map { case (key, value) => (Integer.valueOf(key), value.map(i => i.intValue()).toArray)
      }
    //    groupDerivedCache = Map[Int, Array[Int]]()
    //    groupDerivedCacheBuilder.foreach(kv => groupDerivedCache.set(kv._1, kv._2.map(i => i.intValue()).toArray))

    //The inverseDimensionsCache is constructed from the dimensions cache
    val columns = this.dimensions.getColumns
    val outer = new mutable.HashMap[String, mutable.HashMap[AnyRef, mutable.HashSet[Integer]]]
    for (i <- (1+tidCounter.get()) until (timeSeriesMembersCache.length + tidCounter.get())) {
      //If data with existing tids are ingested missing tids can occur and must be handled
      if (timeSeriesMembersCache.get(i) == null) {
        Static.warn(f"CORE: a time series with tid $i does not exist")
      } else {
        for (j <- 0 until columns.length) {
          val value = timeSeriesMembersCache.get(i)(j)
          val inner = outer.getOrElse(columns(j), mutable.HashMap[AnyRef, mutable.HashSet[Integer]]())
          val tids = inner.getOrElse(value, mutable.HashSet[Integer]())
          tids.add(timeSeriesGroupCache.get(i))
          inner.put(value, tids)
          outer.put(columns(j), inner)
        }
      }
    }
    this.memberTimeSeriesCache = mutable.HashMap[String, mutable.HashMap[AnyRef, Array[Integer]]]()
    for (oes <- outer) {
      val innerAsArray = mutable.HashMap[AnyRef, Array[Integer]]()
      for (ies <- oes._2) {
        innerAsArray.put(ies._1, ies._2.toArray.sorted) //Sorted to make it simpler to read when debugging
      }
      //Some engines converts all columns to uppercase so the caches key must also be so
      this.memberTimeSeriesCache.put(oes._1.toUpperCase, innerAsArray)
    }

    //Finally the sorted groupMetadataCache is created and consists of sampling interval and tids
    groupMetadataCache = new ArrayCache[Array[Int]](gsc.size + 1, gidOffset)
    gsc.foreach(kv => {
      val key = kv._1 //gid
      // metadata = [samplingInterval, tid1, tid2, ...]
      val metadataArray = kv._2.map((i: Integer) => i.intValue()).toArray
      // skip first element (sampling interval) and sort (in-place) remaining elements (tids)
      java.util.Arrays.sort(metadataArray, 1, metadataArray.length)
      groupMetadataCache.set(key, metadataArray)
    })
    storeModelTypes(modelTypesToBeInserted)
  }

  /** Protected Methods * */
  protected def storeTimeSeries(timeSeriesGroups: Array[TimeSeriesGroup], gidOffset: Int): Unit
  protected def getTimeSeries: mutable.HashMap[Integer, Array[AnyRef]]
  protected def storeModelTypes(modelsToInsert: mutable.HashMap[String, Integer]): Unit
  protected def getModelTypes: mutable.HashMap[String, Integer]

  protected def getDimensionsSQL(dimensions: Dimensions, textType: String): String = {
    val columns = dimensions.getColumns
    val types = dimensions.getTypes
    if (types.isEmpty) {
      return ""
    }

    //The schema is build with a starting comma so it is easy to embed into a CREATE table statement
    val sb = new StringBuilder
    sb.append(", ")
    val withPunctuation = columns.length - 1
    for (i <- 0 until withPunctuation) {
      sb.append(columns(i))
      sb.append(' ')
      if (types(i) eq Dimensions.Types.TEXT) sb.append(textType)
      else sb.append(types(i).toString)
      sb.append(", ")
    }
    sb.append(columns(withPunctuation))
    sb.append(' ')
    if (types(withPunctuation) eq Dimensions.Types.TEXT) sb.append(textType)
    else sb.append(types(withPunctuation).toString)
    sb.toString
  }

  /** Instance Variables * */
  var dimensions: Dimensions = _


  //Write Cache: Maps the name of a model type to the corresponding mtid used by the storage layer
  var mtidCache: mutable.HashMap[String, Integer] = _

  //Read Cache: Maps the mtid of a model type to an instance of the model type so segments can be constructed from it
  var modelTypeCache: Array[ModelType] = _


  //Read Cache: Maps the tid of a time series to the gid of the group that the time series is a member of
  var timeSeriesGroupCache: ArrayCache[Int] = _

  //Read Cache: Maps the tid of a time series to the sampling interval specified for that time series
  var timeSeriesSamplingIntervalCache: ArrayCache[Int] = _

  //Read Cache: Maps the tid of a time series to the scaling factor specified for for that time series
  var timeSeriesScalingFactorCache: ArrayCache[Float] = _

  //Read Cache: Maps the tid of a time series to the transformation specified for that time series
  var timeSeriesTransformationCache: ArrayCache[ValueFunction] = _

  //Read Cache: Maps the tid of a time series to the members specified for that time series
  var timeSeriesMembersCache: ArrayCache[Array[AnyRef]] = _

  //Read Cache: Maps the value of a column for a dimension to the tids with that member
  var memberTimeSeriesCache: mutable.HashMap[String, mutable.HashMap[AnyRef, Array[Integer]]] = _

  //Read Cache: Maps the gid of a group to the groups sampling interval and the tids that are part of that group
  var groupMetadataCache: ArrayCache[Array[Int]] = _

  //Read Cache: Maps the gid of a group to pairs of tids for time series with derived time series
  var groupDerivedCache: mutable.HashMap[Integer, Array[Int]] = _
}
