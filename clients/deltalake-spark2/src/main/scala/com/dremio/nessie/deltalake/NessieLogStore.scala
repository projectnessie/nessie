/**
 * Copyright (C) 2020 Dremio
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
package com.dremio.nessie.deltalake

import java.io.{BufferedReader, FileNotFoundException, InputStreamReader}
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.FileAlreadyExistsException
import java.util.UUID

import com.dremio.nessie.client.NessieClient
import com.dremio.nessie.client.NessieClient.AuthType
import com.dremio.nessie.error.NessieNotFoundException
import com.dremio.nessie.model.{ContentsKey, DeltaLakeTable, ImmutableDeltaLakeTable}
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.delta.storage.LogStore
import org.apache.spark.sql.delta.util.FileNames
import org.apache.spark.sql.delta.{DeltaFileType, LogFileMeta}

import scala.collection.JavaConverters._
import scala.util.Try

class NessieLogStore(sparkConf: SparkConf, hadoopConf: Configuration)
  extends LogStore with Logging {

  /*
  todo
  better table/key name?
  does read need adjustment for Nessie?
   */
  val CONF_NESSIE_URL = "nessie.url"
  val CONF_NESSIE_USERNAME = "nessie.username"
  val CONF_NESSIE_PASSWORD = "nessie.password"
  val CONF_NESSIE_AUTH_TYPE = "nessie.auth_type"
  val NESSIE_AUTH_TYPE_DEFAULT = "BASIC"
  val CONF_NESSIE_REF = "nessie.ref"

  private val client: NessieClient = {
    val authType = AuthType.valueOf(hadoopConf.get(CONF_NESSIE_AUTH_TYPE, NESSIE_AUTH_TYPE_DEFAULT))
    val username = hadoopConf.get(CONF_NESSIE_USERNAME)
    val password = hadoopConf.get(CONF_NESSIE_PASSWORD)
    val url = hadoopConf.get(CONF_NESSIE_URL)
    new NessieClient(authType, url, username, password)
  }

  private def getOrCreate() = {
    val requestedRef = hadoopConf.get(CONF_NESSIE_REF)

    try {
      Option(requestedRef).map(client.getTreeApi.getReferenceByName(_)).getOrElse(client.getTreeApi.getDefaultBranch)
    } catch {
      case ex: NessieNotFoundException =>
        if (requestedRef != null) throw new IllegalArgumentException(s"Nessie ref $requestedRef provided " +
          s"via $CONF_NESSIE_REF does not exist. This ref must exist before creating a NessieCatalog.", ex)
        throw new IllegalArgumentException(s"Nessie does not have an existing default branch. Either configure " +
          s"an alternative ref via $CONF_NESSIE_REF or create the default branch on the server.", ex)
    }
  }

  private var branch = getOrCreate()

  override def listFrom(path: Path): Iterator[FileStatus] = {
    throw new UnsupportedOperationException("listFrom from Nessie does not work.")
  }

  def makePair(path: FileStatus): (FileStatus, String) = {
    (path, stripPrefixFromPath(path.getPath))
  }

  def stripPrefixFromPath(path: Path): String = {
    val parts = path.getName.split("-", 2)
    if (Try(parts(0).toLong).isFailure) parts(1) else path.getName
  }

  override def write(path: Path, actions: Iterator[String], overwrite: Boolean = false): Unit = {
    val parent = path.getParent
    val name = s"${UUID.randomUUID().toString.replace("-","")}-${path.getName}"
    val nessiePath = new Path(parent, name)

    writeWithRename(nessiePath, actions, overwrite)
  }

  private def commit(path: Path, message: String = "delta commit"): Boolean = {
    val table = ImmutableDeltaLakeTable.builder().metadataLocation(path.toString).build()
    client.getContentsApi.setContents(pathToKey(path.getParent), branch.getName, branch.getHash, message, table)
    branch = client.getTreeApi.getReferenceByName(branch.getName)
    true
  }

  protected def writeWithRename(path: Path, actions: Iterator[String], overwrite: Boolean = false): Unit = {
    val fs = path.getFileSystem(hadoopConf)

    if (!fs.exists(path.getParent)) {
      throw new FileNotFoundException(s"No such file or directory: ${path.getParent}")
    }
    if (overwrite) {
      val stream = fs.create(path, true)
      try {
        actions.map(_ + "\n").map(_.getBytes(UTF_8)).foreach(stream.write)
      } finally {
        stream.close()
      }
    } else {
      if (fs.exists(path)) {
        throw new FileAlreadyExistsException(path.toString)
      }
      var streamClosed = false // This flag is to avoid double close
      var commitDone = false // This flag is to save the delete operation in most of cases.
      val stream = fs.create(path)
      try {
        actions.map(_ + "\n").map(_.getBytes(UTF_8)).foreach(stream.write)
        stream.close()
        streamClosed = true
        try {
          commitDone = commit(path)
        } catch {
          case _: org.apache.hadoop.fs.FileAlreadyExistsException =>
            throw new FileAlreadyExistsException(path.toString)
        }
      } finally {
        if (!streamClosed) {
          stream.close()
        }
        if (!commitDone) {
          fs.delete(path, false)
        }
      }
    }
  }

  def numCheckpointParts(path: Path): Option[Int] = {
    val segments = path.getName.split("\\.")

    segments match {
      case x: Array[String] if x.length == 6 => Some(segments(4).toInt)
      case _ => None
    }
  }

  def pathToKey(path: Path): ContentsKey = {
    val parts = path.toUri.getPath.split("/").toList
    new ContentsKey(parts.asJava)
  }

  def extractMeta(fileStatus: FileStatus): LogFileMeta = {
    val strippedPath = new Path(stripPrefixFromPath(fileStatus.getPath))
    LogFileMeta(fileStatus,
      FileNames.getFileVersion(strippedPath),
      DeltaFileType.getFileType(strippedPath),
      numCheckpointParts(strippedPath))
  }

  def listFilesFrom(path: Path): Iterator[LogFileMeta] = {
    val fs = path.getFileSystem(hadoopConf)
    if (!fs.exists(path.getParent)) {
      throw new FileNotFoundException(s"No such file or directory: ${path.getParent}")
    }

    val table = client.getContentsApi.getContents(pathToKey(path.getParent), branch.getName)

    if (table == null || !table.isInstanceOf[DeltaLakeTable]) {
      throw new FileNotFoundException(s"No such table: $path")
    }
    val currentPath = new Path(table.asInstanceOf[DeltaLakeTable].getMetadataLocation)
    val currentVersion = FileNames.getFileVersion(new Path(stripPrefixFromPath(currentPath)))
    val requestedVersion = Try(FileNames.getFileVersion(path)).getOrElse(path.getName.stripSuffix(".checkpoint").toLong)
    val files = fs.listStatus(path.getParent)
    files.map(extractMeta)
      .filter(_.version <= currentVersion)
      .filter(_.version >= requestedVersion)
      .sortBy(p => stripPrefixFromPath(p.fileStatus.getPath)).iterator
  }

  override def read(path: Path): Seq[String] = {
    val fs = path.getFileSystem(hadoopConf)
    val stream = fs.open(path)
    try {
      val reader = new BufferedReader(new InputStreamReader(stream, UTF_8))
      IOUtils.readLines(reader).asScala.map(_.trim)
    } finally {
      stream.close()
    }
  }

  override def invalidateCache(): Unit = {}
}
