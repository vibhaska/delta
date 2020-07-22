/*
 * Copyright (2020) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.oci

import java.net.URI

import org.apache.hadoop.fs.Path

class FileOnOCI(val path: Path) {
  require(path.toUri.isAbsolute && path.toUri.getScheme.equals("oci"), "Schema should be 'oci'.")

  def this(uri: URI) = this(new Path(uri))

  def this(file: FileOnOCI, child: String) = this(new Path(file.path.toUri.getScheme,
    file.path.toUri.getAuthority, s"${file.path.toUri.getPath}/$child"))

  def getName: String = path.getName

  def getPath: String = path.toUri.getRawPath

  def getAbsolutePath: String = path.toString

  def getCanonicalPath: String = getAbsolutePath

  override def toString: String = getAbsolutePath

  override def equals(obj: Any): Boolean = obj match {
    case f: FileOnOCI => path.compareTo(f.path) == 0
    case _ => false
  }

  override def hashCode(): Int = path.hashCode() ^ 1234321
}
