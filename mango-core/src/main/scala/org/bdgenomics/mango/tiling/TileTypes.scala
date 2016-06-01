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
package org.bdgenomics.mango.tiling

import org.bdgenomics.formats.avro.Genotype
import org.bdgenomics.mango.layout.{ CalculatedAlignmentRecord, ConvolutionalSequence }

case class AlignmentRecordTile(alignments: Array[CalculatedAlignmentRecord],
                               layer1: Map[String, Array[Double]],
                               keys: List[String]) extends KLayeredTile[Array[CalculatedAlignmentRecord]] with Serializable {

  // TODO map to samples
  val rawData = alignments.groupBy(_.record.getRecordGroupSample)

  // TODO: verify layer sizes
  val layer2 = layer1.mapValues(r => ConvolutionalSequence.convolveArray(r, L1.patchSize, L1.stride))
  val layer3 = layer2.mapValues(r => ConvolutionalSequence.convolveArray(r, L1.patchSize, L1.stride))
  val layer4 = layer3.mapValues(r => ConvolutionalSequence.convolveArray(r, L1.patchSize, L1.stride))

  val layerMap = Map(1 -> layer1.mapValues(_.map(_.toByte)),
    2 -> layer2.mapValues(_.map(_.toByte)),
    3 -> layer3.mapValues(_.map(_.toByte)),
    4 -> layer4.mapValues(_.map(_.toByte)))
}

case class ReferenceTile(sequence: String) extends LayeredTile[String] with Serializable {
  val rawData = sequence
  val layerMap = ConvolutionalSequence.convolveToEnd(sequence, LayeredTile.layerCount)
}

case class VariantTile(variants: Array[Genotype]) extends LayeredTile[Array[Genotype]] with Serializable {
  val rawData = variants

  L0.maxSize = 500
  L1.maxSize = 1000
  L2.maxSize = 5000
  L3.maxSize = 10000
  L4.maxSize = 100000

  //TODO: Is this byte necessary?
  val layer1 = rawData.filter(r => r.getVariant.getStart % L1.stride == 0).map(_.toString.toByte)
  val layer2 = rawData.filter(r => r.getVariant.getStart % L2.stride == 0).map(_.toString.toByte)
  val layer3 = rawData.filter(r => r.getVariant.getStart % L3.stride == 0).map(_.toString.toByte)
  val layer4 = rawData.filter(r => r.getVariant.getStart % L4.stride == 0).map(_.toString.toByte)

  val layerMap: Map[Int, Array[Byte]] = Map(1 -> layer1, 2 -> layer2, 3 -> layer3, 4 -> layer4)
}