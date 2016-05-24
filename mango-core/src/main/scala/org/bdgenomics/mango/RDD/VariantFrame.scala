/**
 * Licensed to Big Data Genomics (BDG) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The BDG licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bdgenomics.mango.RDD

import org.apache.spark.{ SparkContext, Logging }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ SQLContext, Row, DataFrame }
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.mango.core.util.VizUtils
import org.bdgenomics.mango.util.Bookkeep
import scala.collection.mutable.HashMap

class VariantFrame(sc: SparkContext) extends Logging {

  val sqlContext = new SQLContext(sc)

  var fileMap: HashMap[String, DataFrame] = new HashMap()
  var sampMap: Option[Map[String, Int]] = None

  var freqBook: Bookkeep = new Bookkeep(1000)
  var varBook: Bookkeep = new Bookkeep(1000)

  val varSchema =
    StructType(
      StructField("start", LongType, true) ::
        StructField("sampleId", StringType, true) ::
        StructField("alleles", ArrayType(StringType, false), false) :: Nil)

  var wsetFreq: DataFrame = sqlContext.createDataFrame(sc.emptyRDD[Counts])
  var wsetVar: DataFrame = sqlContext.createDataFrame(sc.emptyRDD[Row], varSchema)

  /*
   * Stores the filemap of variant information for each chromosome. Each file is stored as a dataframe
   */
  def loadChr(filePath: String): Unit = {
    println("loading")
    println(filePath)
    val df: DataFrame = sqlContext.read.load(filePath)
    val chr: String = df.first.get(1).asInstanceOf[String] //TODO: should be contigName, correct based on schema
    //TODO: perform schema projection here instead of in the get functions?
    println(fileMap.keySet)
    fileMap += ((chr, df))
    println("after insert")
    println(fileMap.keySet)
  }

  /*
   * Gets the dataframe associated with each file
   */
  def getDF(chr: String): Option[DataFrame] = {
    println(fileMap)
    fileMap.get(chr)
  }

  /*
   * Gets the frequency information for a region, formatted as JSON
   */
  def getFreq(region: ReferenceRegion): Array[String] = {
    if (fetchVarFreqData(region)) { //file exists for what we're querying for
      val binSize = VizUtils.getBinSize(region, 1000)
      println(binSize)
      wsetFreq.filter(wsetFreq("start") >= region.start && wsetFreq("start") <= region.end
        && wsetFreq("start") % binSize === 0).toJSON.collect
    } else {
      println("RETURNING NOTHING")
      Array[String]() //return empty array
    }
  }

  /*
   * Materializes a region of frequency data. This is used for both regular fetching
   * Note that no filtering is done on contigName because we assume variant files are per chromosome
   */
  def fetchVarFreqData(region: ReferenceRegion, prefetch: Boolean = false): Boolean = {
    val df = getDF(region.referenceName)
    df match {
      case Some(_) => {
        //TODO: contains all the samples right now
        val matRegions: Option[List[ReferenceRegion]] = freqBook.getMaterializedRegions(region, List("all"))
        if (matRegions.isDefined) {
          for (reg <- matRegions.get) {
            println(region)
            val filt = df.get.select("start", "alleles")
            val counts = filt.filter(filt("start") >= reg.start && filt("start") <= reg.end).groupBy("start").count
            wsetFreq = wsetFreq.unionAll(counts)
            freqBook.rememberValues(reg, "all") //TODO: contains all the samples right now
            wsetFreq.cache
            if (prefetch) wsetFreq.count
          }
        } else {
          println("ALREADY FETCHED BEFORE")
        }
        true
      }
      case None => {
        println("FILE NOT PROVIDED FOR FREQ")
        false
      }
    }
  }

  /*
   * Gets the variant data for a region, formatted as JSON
   */
  def get(region: ReferenceRegion): Array[VariantJson] = {
    if (fetchVarData(region)) { //file exists for what we're querying for
      val total = wsetVar.filter(wsetVar("start") >= region.start && wsetVar("start") <= region.end)
      val data: Array[Row] = total.collect
      val grouped: Map[(String, Array[org.apache.spark.sql.Row]), Int] = data.groupBy(_.get(1).asInstanceOf[String]).zipWithIndex

      //Note: This assumes point mutations for now, so records are only filtered by start field
      grouped.flatMap(recs => recs._1._2.map(r => new VariantJson(region.referenceName, recs._1._1, r.get(0).asInstanceOf[Long],
        r.get(0).asInstanceOf[Long] + 1, r.get(2).asInstanceOf[Seq[String]].mkString(","), recs._2))).toArray
    } else {
      Array[VariantJson]() //return empty array
    }
  }

  /*
   * Materializes a region of variant data. This is used for both regular fetching
   */
  def fetchVarData(region: ReferenceRegion, prefetch: Boolean = false): Boolean = {
    val df = getDF(region.referenceName)
    df match {
      case Some(_) => {
        //TODO: contains all the samples right now
        val matRegions: Option[List[ReferenceRegion]] = varBook.getMaterializedRegions(region, List("all"))
        if (matRegions.isDefined) {
          for (reg <- matRegions.get) {
            val filt = df.get.select("start", "sampleId", "alleles")
            val data = filt.filter(filt("start") >= reg.start && filt("start") <= reg.end)
            wsetVar = wsetVar.unionAll(data)
            varBook.rememberValues(reg, "all") //TODO: contains all the samples right now
            wsetVar.cache
            if (prefetch) wsetVar.count
          }
        }
        true
      }
      case None => {
        println("FILE NOT PROVIDED FOR VAR")
        false
      }
    }
  }

}

// tracked json objects for genotype visual data
case class VariantJson(contigName: String, sampleId: String, start: Long, end: Long, alleles: String, track: Int)
case class Counts(start: Long, count: Int)
