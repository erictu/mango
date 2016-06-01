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

case class VariantTile(variants: Array[Genotype]) extends LayeredTile[Genotype] with Serializable {

  val rawData = variants

  L0.maxSize = 500
  L1.maxSize = 1000
  L2.maxSize = 5000
  L3.maxSize = 10000
  L4.maxSize = 100000

  val layer1 = rawData.filter(r => r.getVariant.getStart.asInstanceOf[Int] % L1.stride == 0)
  val layer2 = rawData.filter(r => r.getVariant.getStart.asInstanceOf[Int] % L2.stride == 0)
  val layer3 = rawData.filter(r => r.getVariant.getStart.asInstanceOf[Int] % L3.stride == 0)
  val layer4 = rawData.filter(r => r.getVariant.getStart.asInstanceOf[Int] % L4.stride == 0)

  val layerMap: Map[Int, Array[Genotype]] = Map(1 -> layer1, 2 -> layer2, 3 -> layer3, 4 -> layer4)

}