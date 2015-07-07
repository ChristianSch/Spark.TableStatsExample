package com.cloudera.sa.examples.tablestats.model

import scala.collection.mutable

/**
 * Created by ted.malaska on 6/29/15.
 */
class FirstPassStatsModel extends Serializable {
  var columnStatsMap = new mutable.HashMap[Integer, ColumnStats]

  def +=(colIndex: Int, colValue: Any, colCount: Long): Unit = {
    columnStatsMap.getOrElseUpdate(colIndex, new ColumnStats) += (colValue, colCount)
  }

  def +=(firstPassStatsModel: FirstPassStatsModel): Unit = {
    firstPassStatsModel.columnStatsMap.foreach{ case(idx, stats) =>
      columnStatsMap.get(idx) match {
        case Some(prevStats) =>
          prevStats += stats
        case None =>
          columnStatsMap(idx) = stats
      }
    }
  }

  override def toString = s"FirstPassStatsModel(columnStatsMap=$columnStatsMap)"
}
