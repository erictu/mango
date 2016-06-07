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

import edu.berkeley.cs.amplab.spark.intervalrdd.IntervalRDD
import org.apache.parquet.filter2.dsl.Dsl._
import org.apache.parquet.filter2.predicate.FilterPredicate
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.bdgenomics.adam.models.{ ReferencePosition, ReferenceRegion, SequenceDictionary }
import org.bdgenomics.adam.projections.{ GenotypeField, Projection }
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.variation._
import org.bdgenomics.formats.avro.Genotype
import org.bdgenomics.mango.tiling.VariantTile
import org.bdgenomics.mango.util.Bookkeep

import scala.reflect.ClassTag

/*
 * Handles loading and tracking of data from persistent storage into memory for Genotype data.
 * @see LazyMaterialization.scala
 */
class GenotypeMaterialization(s: SparkContext, d: SequenceDictionary, parts: Int, chunkS: Int) extends LazyMaterialization[Genotype, VariantTile] {

  val sc = s
  val dict = d
  val partitions = parts
  val chunkSize = chunkS
  val partitioner = setPartitioner
  val bookkeep = new Bookkeep(chunkSize)

  def loadAdam(region: ReferenceRegion, fp: String): RDD[Genotype] = {
    val pred: FilterPredicate = ((LongColumn("end") >= region.start) && (LongColumn("start") <= region.end) && (BinaryColumn("contigName") === (region.referenceName)))
    val proj = Projection(GenotypeField.start, GenotypeField.end, GenotypeField.alleles, GenotypeField.sampleId)
    sc.loadParquetGenotypes(fp, predicate = Some(pred), projection = Some(proj))
    val genes = sc.loadParquetGenotypes(fp, predicate = Some(pred))
    genes
  }

  override def getFileReference(fp: String): String = fp

  def loadFromFile(region: ReferenceRegion, k: String): RDD[Genotype] = {
    if (!fileMap.containsKey(k)) {
      log.error("Key not in FileMap")
      null
    }
    val fp = fileMap(k)
    if (fp.endsWith(".adam")) {
      println("LOADING FROM ADAM")
      loadAdam(region, fp)
    } else if (fp.endsWith(".vcf")) {
      sc.loadGenotypes(fp).filterByOverlappingRegion(region)
    } else {
      throw UnsupportedFileException("File type not supported")
      null
    }
  }

  /**
   *  Transparent to the user, should only be called by get if IntervalRDD.get does not return data
   * Fetches the data from disk, using predicates and range filtering
   * Then puts fetched data in the IntervalRDD, and calls multiget again, now with the data existing
   *
   * @param region ReferenceRegion in which data is retreived
   * @param ks to be retreived
   */
  override def put(region: ReferenceRegion, ks: List[String]) = {
    val seqRecord = dict(region.referenceName)
    seqRecord match {
      case Some(_) =>
        val end =
          Math.min(region.end, seqRecord.get.length)
        val start = Math.min(region.start, end)
        val trimmedRegion = new ReferenceRegion(region.referenceName, start, end)
        var data: RDD[Genotype] = sc.emptyRDD[Genotype]

        //divide regions by chunksize
        val c = chunkSize
        val regions: List[ReferenceRegion] = Bookkeep.unmergeRegions(region, chunkSize)

        ks.map(k => {
          val kdata = loadFromFile(trimmedRegion, k)
          data = data.union(kdata)
        })

        var mappedRecords: RDD[(ReferenceRegion, Genotype)] = sc.emptyRDD[(ReferenceRegion, Genotype)]

        regions.foreach(r => {
          val grouped = data.filter(vr => r.overlaps(ReferenceRegion(vr.getContigName, vr.getStart, vr.getEnd)))
            .map(vr => (r, vr))
          mappedRecords = mappedRecords.union(grouped)
        })

        val groupedRecords: RDD[(ReferenceRegion, Iterable[Genotype])] =
          mappedRecords
            .groupBy(_._1)
            .map(r => (r._1, r._2.map(_._2)))
        val tiles: RDD[(ReferenceRegion, VariantTile)] = groupedRecords.map(r => (r._1, VariantTile(r._2, trimmedRegion)))

        // insert into IntervalRDD
        if (intRDD == null) {
          intRDD = IntervalRDD(tiles)
          intRDD.persist(StorageLevel.MEMORY_AND_DISK)
        } else {
          val t = intRDD
          intRDD = intRDD.multiput(tiles)
          // TODO: can we do this incrementally instead?
          t.unpersist(true)
          intRDD.persist(StorageLevel.MEMORY_AND_DISK)
        }
        bookkeep.rememberValues(region, ks)
      case None =>
    }
  }
}

object GenotypeMaterialization {

  def apply(sc: SparkContext, dict: SequenceDictionary, partitions: Int): GenotypeMaterialization = {
    new GenotypeMaterialization(sc, dict, partitions, 100)
  }

  def apply[T: ClassTag, C: ClassTag](sc: SparkContext, dict: SequenceDictionary, partitions: Int, chunkSize: Int): GenotypeMaterialization = {
    new GenotypeMaterialization(sc, dict, partitions, chunkSize)
  }
}