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
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.formats.avro.Genotype

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

object VariantLayout extends Logging {

  /**
   * An implementation of Variant Layout
   *
   * @param genotypes: Array of Genotype
   * @return List of VariantJsons
   */
  def apply(genotypes: Array[Genotype]): Iterable[SampleGenotype] = {
    val variantData: Map[String, Array[Genotype]] = genotypes.groupBy(_.getSampleId)
    variantData.map(r => SampleGenotype(r._1, r._2.map(m => Mutation(m.getAlleles.mkString("/"), m.getStart, m.getEnd))))
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
   * @param genotypes: Iterable of (ReferenceRegion, Genotype) tuples
   * @return List of VariantFreq
   */
  def apply(genotypes: Iterable[Genotype]): List[VariantFreq] = {
    // TODO: enumerate alleles
    val keyed = genotypes.groupBy(_.getStart).map(r => (r._1, r._2.map(_.getAlleles.mkString("/"))))
    keyed.map(r => {
      val alleles = r._2.groupBy(identity).map(r => (r._1, r._2.size))
      VariantFreq(r._1, alleles)
    }).toList
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

// tracked json objects for genotype visual data
case class SampleGenotype(sampleId: String, mutations: Array[Mutation])
case class Mutation(alleles: String, start: Long, end: Long)

case class VariantFreq(start: Long, alleleCounts: Map[String, Int])
