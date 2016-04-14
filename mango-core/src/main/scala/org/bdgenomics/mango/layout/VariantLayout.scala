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
package org.bdgenomics.mango.layout

import org.apache.spark.{ SparkContext, Logging }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ SQLContext, Row, DataFrame }
import org.apache.spark.storage.StorageLevel
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.formats.avro.Genotype
import org.bdgenomics.mango.util.Bookkeep

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

class VariantLayout(sc: SparkContext) extends Logging {

  val sqlContext = new org.apache.spark.sql.SQLContext(sc)

  var fileMap: Map[String, DataFrame] = Map[String, DataFrame]()

  var freqBook: Bookkeep = new Bookkeep(1000)
  var varBook: Bookkeep = new Bookkeep(1000)

  var wsetFreq: DataFrame = sqlContext.createDataFrame(sc.emptyRDD[Counts])
  var wsetVar: DataFrame = sqlContext.createDataFrame(sc.emptyRDD[VariantSchema])

  def loadChr(filePath: String): Unit = {
    val df: DataFrame = sqlContext.read.load(filePath)
    val chr: String = df.first.get(1).asInstanceOf[String] //TODO: should be contigName, correct based on schema
    //TODO: perform schema projection here instead of in the get functions?
    fileMap += (chr -> df)
  }

  def getDF(chr: String): Option[DataFrame] = {
    fileMap.get(chr)
  }

  def getFreq(region: ReferenceRegion): Array[String] = {
    val df = getDF(region.referenceName)
    df match {
      case Some(_) => {
        //TODO: contains all the samples right now
        val matRegions: Option[List[ReferenceRegion]] = freqBook.getMaterializedRegions(region, List("all"))
        if (!matRegions.isEmpty) {
          for (reg <- matRegions.get) {
            val filt = df.get.select("variant__contig__contigName", "variant__start", "sampleId")
            val counts = filt.filter(filt("variant__start") >= reg.start && filt("variant__start") <= reg.end).groupBy("variant__start").count
            wsetFreq = wsetFreq.unionAll(counts)
            freqBook.rememberValues(reg, "all") //TODO: contains all the samples right now
            wsetFreq.cache
          }
        }
        //Fetching data from working set
        wsetFreq.filter(wsetFreq("variant__start") >= region.start && wsetFreq("variant__start") <= region.end).toJSON.collect
      }
      case None => {
        println("FILE NOT PROVIDED")
        Array[String]() //return empty array
      }
    }
  }

  def get(region: ReferenceRegion): List[VariantJson] = {
    val df = getDF(region.referenceName)
    df match {
      case Some(_) => {
        //TODO: contains all the samples right now
        val matRegions: Option[List[ReferenceRegion]] = varBook.getMaterializedRegions(region, List("all"))
        if (!matRegions.isEmpty) {
          for (reg <- matRegions.get) {
            val filt = df.get.select("variant__contig__contigName", "variant__start", "sampleId")
            val data = filt.filter(filt("variant__start") >= reg.start && filt("variant__start") <= reg.end)
            wsetVar = wsetVar.unionAll(data)
            varBook.rememberValues(reg, "all") //TODO: contains all the samples right now
            wsetVar.cache
          }
        }
        //Fetching data from working set
        val total = wsetVar.filter(wsetVar("variant__start") >= region.start && wsetVar("variant__start") <= region.end)
        val zipped: RDD[((String, Iterable[Row]), Long)] = wsetVar.rdd.groupBy(r => r.get(3).asInstanceOf[String]).zipWithIndex
        zipped.flatMap(recs => recs._1._2.map(r => new VariantJson(r.get(0).toString, recs._1._1, r.get(1).asInstanceOf[Long],
          r.get(2).asInstanceOf[Long], recs._2))).collect.toList
      }
      case None => {
        println("FILE NOT PROVIDED")
        List[VariantJson]() //return empty array
      }
    }
  }
}

// tracked json objects for genotype visual data
case class VariantJson(contigName: String, sampleId: String, start: Long, end: Long, track: Long)
case class VariantSchema(variant__contig__contigName: String, variant__start: Long, sampleId: String)
case class Counts(variant__start: Long, count: Int)
