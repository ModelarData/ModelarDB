package dk.aau.modelardb

import enumeratum._

sealed trait OffsetType extends EnumEntry
object OffsetType extends Enum[OffsetType] {

  val values = findValues

  case object TID extends OffsetType
  case object GID extends OffsetType

}
