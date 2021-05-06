package dk.aau.modelardb.core.models

import java.util

import dk.aau.modelardb.core.DataPoint

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers


class PMC_MeanModelTypeTest2 extends AnyFunSuite with Matchers {

    test("Initialize") {
        val d1 = new DataPoint(1, 123L, 13.0f)
        val d2 = new DataPoint(2, 124L, 12.0f)
        val d3 = new DataPoint(3, 125L, 14.0f)
        val array = Array(d1, d2, d3)
        val list = util.Arrays.asList(array)
        val model = new PMC_MeanModelType(12, 10, 10)
        model.initialize(list)
        model.length() should equal (1)
    }
}
