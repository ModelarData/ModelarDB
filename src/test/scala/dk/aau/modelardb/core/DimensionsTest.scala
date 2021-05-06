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
package dk.aau.modelardb.core

import dk.aau.modelardb.core.timeseries.{TimeSeries, TimeSeriesCSV}
import org.scalatest.wordspec.AnyWordSpec

//TODO: split this into tests for Dimension, Correlation, and Partitioner instead of testing all three here
class DimensionsTest extends AnyWordSpec {

  "initialized with two dimensions containing two levels each" should {
    val dimensions = new Dimensions(Array(
      "Location, house string, channel string",
      "Type, purpose string, building string"
    ))

    dimensions.add("house_1-channel_1.dat, House 1, Channel 1, Housing, Apartment")
    dimensions.add("house_1-channel_2.dat, House 1, Channel 2, Housing, Apartment")
    dimensions.add("house_2-channel_1.dat, House 2, Channel 1, Housing, Apartment")
    dimensions.add("house_2-channel_2.dat, House 2, Channel 2, Housing, Apartment")
    dimensions.add("house_3-channel_1.dat, House 3, Channel 1, Housing, Farm")
    dimensions.add("house_3-channel_2.dat, House 3, Channel 2, Housing, Farm")
    dimensions.add("house_4-channel_1.dat, House 4, Channel 1, Housing, Apartment")
    dimensions.add("house_4-channel_2.dat, House 4, Channel 2, Housing, Apartment")
    dimensions.add("house_5-channel_1.dat, House 5, Channel 1, Housing, Farm")
    dimensions.add("house_5-channel_2.dat, House 5, Channel 2, Housing, Farm")

    "contain two dimensions" in {
      assert(dimensions.getDimensions.size() == 2)
    }

    "contain four columns" in {
      assert(dimensions.getColumns.length == 4)
    }

    "contain ten rows" in {
      assert(dimensions.getSources.length == 10)
    }

    //TODO: Create a proper test time series that can generate N data points based on a function, .e.g, a sine curve
    val timeSeries: Array[TimeSeries] = dimensions.getSources.map(source => new TimeSeriesCSV(source, 1, 1000, " ", false,
      0, "unix", "UTC", 1, "en"))

    "create nine time series groups based on correlation by source" in {
      val configuration = new Configuration()
      configuration.add("modelardb.batch_size", 500)
      configuration.add("modelardb.dimensions", dimensions)
      val correlation = new Correlation()
      correlation.addSources(Array("house_1-channel_1.dat", "house_1-channel_2.dat"))
      configuration.add("modelardb.correlations", Array(correlation))
      val groups = Partitioner.groupTimeSeries(configuration, timeSeries, 0)
      assert(groups.length == 9)
    }

    "create five time series groups based on correlation by dimensions" in {
      val configuration = new Configuration()
      configuration.add("modelardb.batch_size", 500)
      configuration.add("modelardb.dimensions", dimensions)
      val correlation = new Correlation()
      correlation.addDimensionAndLCA("Type", 0, dimensions)
      correlation.addDimensionAndLCA("Location", 1, dimensions)
      configuration.add("modelardb.correlations", Array(correlation))
      val groups = Partitioner.groupTimeSeries(configuration, timeSeries, 0)
      assert(groups.length == 5)
    }

    "create five time series groups based on correlation by distance" in {
      val configuration = new Configuration()
      configuration.add("modelardb.batch_size", 500)
      configuration.add("modelardb.dimensions", dimensions)
      val correlation = new Correlation()
      correlation.setDistance(0.25F)
      configuration.add("modelardb.correlations", Array(correlation))
      val groups = Partitioner.groupTimeSeries(configuration, timeSeries, 0)
      assert(groups.length == 5)
    }
  }
}
