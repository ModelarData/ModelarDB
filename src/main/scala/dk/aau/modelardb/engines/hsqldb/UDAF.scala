package dk.aau.modelardb.engines.hsqldb

//Aggregates in HyperSQL always have four parameters with the first being the input from SQL.
// While the finalize flag is false new values are given as in and register should be updated
// with the result, when finalize becomes true the final result should be computed and returned.
// Types: http://hsqldb.org/doc/2.0/guide/sqlroutines-chapt.html#src_jrt_routines
// UDAF: http://hsqldb.org/doc/2.0/guide/sqlroutines-chapt.html#src_aggregate_functions
object UDAF {
  def countS(stet: Integer, finalize: Boolean, total: Array[Integer], ignore: Array[Integer]): Integer = {
    if (finalize) {
      return 42
    }
    null.asInstanceOf[Int] //The value is ignored but something must be returned
  }
}
