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
package org.bdgenomics.mango.models

import edu.berkeley.cs.amplab.spark.intervalrdd._
import org.apache.spark.rdd.RDD
import org.apache.spark.{ Logging, _ }
import org.bdgenomics.adam.models.{ ReferenceRegion, SequenceDictionary }
import org.bdgenomics.adam.rdd.GenomicRegionPartitioner
import org.bdgenomics.mango.util.Bookkeep

import scala.collection.mutable
import scala.collection.mutable.HashMap
import scala.reflect.ClassTag

abstract class LazyMaterialization[T: ClassTag, S: ClassTag] extends Serializable with Logging {

  def sc: SparkContext
  def dict: SequenceDictionary
  def partitions: Int
  def chunkSize: Int
  def partitioner: Partitioner
  def bookkeep: Bookkeep

  def setPartitioner: Partitioner = {
    if (sc.isLocal)
      new HashPartitioner(partitions)
    else
      GenomicRegionPartitioner(partitions, dict)
  }

  def getDictionary: SequenceDictionary = {
    dict
  }

  // Stores location of sample at a given filepath
  def loadSample(filePath: String, sampleId: Option[String] = None) {
    sampleId match {
      case Some(_) => fileMap += ((sampleId.get, filePath))
      case None    => fileMap += ((filePath, filePath))

    }
  }

  def loadADAMSample(filePath: String): String = {
    val sample = getFileReference(filePath)
    fileMap += ((sample, filePath))
    sample
  }

  // Keeps track of sample ids and corresponding files
  var fileMap: HashMap[String, String] = new HashMap()

  def getFileMap: mutable.HashMap[String, String] = fileMap

  var intRDD: IntervalRDD[ReferenceRegion, S] = null

  def getFileReference(fp: String): String

  def loadFromFile(region: ReferenceRegion, k: String): RDD[T]

  def put(region: ReferenceRegion, ks: List[String])

  //  def get(region: ReferenceRegion, k: String): Option[IntervalRDD[ReferenceRegion, S]] = {
  //    multiget(region, List(k))
  //  }
  //
  //  /* If the RDD has not been initialized, initialize it to the first get request
  //	* Gets the data for an interval for the file loaded by checking in the bookkeeping tree.
  //	* If it exists, call get on the IntervalRDD
  //	* Otherwise call put on the sections of data that don't exist
  //	* Here, ks, is an option of list of personids (String)
  //	*/
  //  def multiget(region: ReferenceRegion, ks: List[String]): Option[IntervalRDD[ReferenceRegion, S]] = {
  //    val seqRecord = dict(region.referenceName)
  //    val regionsOpt = bookkeep.getMaterializedRegions(region, ks)
  //    //    println(regionsOpt.get)
  //    seqRecord match {
  //      case Some(_) =>
  //        regionsOpt match {
  //          case Some(_) =>
  //            for (r <- regionsOpt.get) {
  //              println("PUTTING NEW")
  //              println(r)
  //              put(r, ks)
  //            }
  //          case None =>
  //            println("ALREADY FETCHED")
  //          // DO NOTHING
  //        }
  //        //        println("in multiget")
  //        //        println(region)
  //        //        intRDD.collect.foreach(println(_))
  //        //        intRDD.filterByInterval(region).collect.foreach(println(_))
  //        Option(intRDD.filterByInterval(region))
  //      case None =>
  //        None
  //    }
  //  }

}

case class UnsupportedFileException(message: String) extends Exception(message)