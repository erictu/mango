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

import org.bdgenomics.adam.models.{ ReferenceRegion, SequenceDictionary, SequenceRecord }
import org.bdgenomics.mango.util.MangoFunSuite
import org.bdgenomics.adam.rdd.ADAMContext._
import net.liftweb.json._

class GenotypeMaterializationSuite extends MangoFunSuite {

  implicit val formats = DefaultFormats

  val vcf = resourcePath("truetest.vcf")
  val chunkSize = 10
  val sd = new SequenceDictionary(Vector(SequenceRecord("chr1", 2000L),
    SequenceRecord("chrM", 20000L),
    SequenceRecord("20", 90000L)))

  sparkTest("test across tiles") {
    val region = ReferenceRegion("chrM", 1, 31)
    val region2 = ReferenceRegion("chrM", 35, 52)
    val region3 = ReferenceRegion("chrM", 2, 51)
    val variantData = new GenotypeMaterialization(sc, sd, 10, 1)
    variantData.loadSample(vcf)
    println("reg1")
    val numgot = variantData.multiget(region, List(vcf)).get.toRDD
    val results = numgot.map(_._2).flatMap(_.rawData)
    val c1 = numgot.collect
    println()
    c1.foreach(println(_))
    println()

    println("reg2")
    val numgot2 = variantData.multiget(region2, List(vcf)).get.toRDD
    val results2 = numgot2.map(_._2).flatMap(_.rawData)
    val c2 = numgot2.collect
    println()
    c2.foreach(println(_))
    results2.foreach(println(_))
    println()

    println("reg3")
    val numgot3 = variantData.multiget(region3, List(vcf)).get.toRDD
    val results3 = numgot3.map(_._2).flatMap(_.rawData)
    val c3 = numgot3.collect
    println()
    c3.foreach(println(_))
    println()

    assert(results.count == 2)
    assert(results2.count == 1)
    assert(results3.count == 3)

  }

}
