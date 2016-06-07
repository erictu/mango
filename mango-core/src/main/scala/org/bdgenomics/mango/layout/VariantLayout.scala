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

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.formats.avro.Genotype

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

object VariantLayout extends Logging {

  /**
   * An implementation of Variant Layout
   *
   * @param rdd: RDD of Genotype
   * @return List of VariantJsons
   */
  def apply(rdd: RDD[Genotype]): List[VariantJson] = {
    val variantData: RDD[((String, Iterable[Genotype]), Long)] = rdd.groupBy(_.getSampleId).zipWithIndex()
    variantData.flatMap(recs => recs._1._2.map(r => new VariantJson(r.getContigName, r.getSampleId, r.getAlleles.mkString(" / "),
      r.getStart, r.getEnd, recs._2))).collect.toList
  }

  /**
   * An implementation of Variant Layout
   *
   * @param iter: Iterator of (ReferenceRegion, Genotype) tuples
   * @return List of Genotype Tracks
   */
  def apply(iter: Iterator[(ReferenceRegion, Genotype)]): Iterator[GenericTrack[Genotype]] = {
    new VariantLayout(iter).collect
  }

}

object VariantFreqLayout extends Logging {

  /**
   * An implementation of VariantFreqLayout
   *
   * @return List of VariantFreqJsons
   */
  def apply(data: Iterable[Genotype]): List[(Long, String)] = {
    val keyed: Iterable[(Long, String)] = data.map(r => (r.getStart.asInstanceOf[Long], r.getAlleles.mkString(" / ")))

    var freqJson = new ListBuffer[VariantFreqJson]
    keyed.toList
  }

}

/**
 * An implementation of TrackedLayout for Genotype Data
 *
 * @param values Iterator of (ReferenceRegion, Genotype) tuples
 */
class VariantLayout(values: Iterator[(ReferenceRegion, Genotype)]) extends TrackedLayout[Genotype, GenericTrackBuffer[Genotype]] with Logging {
  val sequence = values.toArray
  var trackBuilder = new ListBuffer[GenericTrackBuffer[Genotype]]()
  val data = sequence.groupBy(_._2.getSampleId)
  addTracks
  trackBuilder = trackBuilder.filter(_.records.nonEmpty)

  def addTracks {
    for (rec <- data) {
      trackBuilder += GenericTrackBuffer[Genotype](rec._2.toList)
    }
  }
  def collect: Iterator[GenericTrack[Genotype]] = trackBuilder.map(t => Track[Genotype](t)).toIterator
}

object VariantJson {

  /**
   * An implementation of VariantJson
   *
   * @param recs: List of (ReferenceRegion, Genotype) tuples
   * @return List of VariantJsons
   */
  def apply(recs: List[(ReferenceRegion, Genotype)], track: Int): List[VariantJson] = {
    recs.map(rec => new VariantJson(rec._2.getVariant.getContigName, rec._2.getSampleId,
      rec._2.getAlleles.map(_.toString).mkString(" / "), rec._2.getStart,
      rec._2.getEnd, track))
  }
}

// tracked json objects for genotype visual data
case class VariantJson(contigName: String, sampleId: String, alleles: String, start: Long, end: Long, track: Long)
case class VariantFreqJson(start: Long, alleles: String, count: Long)
