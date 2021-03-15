package dk.aau.modelardb.core.models

import dk.aau.modelardb.core.DataPoint
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import collection.JavaConverters._


class PMC_MeanModelTest extends AnyFunSuite with Matchers {

    test("Initialize") {
        var d1 = new DataPoint(1, 123L, 13.0f)
        var d2 = new DataPoint(2, 124L, 12.0f)
        var d3 = new DataPoint(3, 125L, 14.0f)
        var array = Array(d1, d2, d3)
        var list = List(array)
        var model = new PMC_MeanModel(12, 10, 10)
        model.initialize(list.asJava)
        model.length() should equal (1)
    }
}
