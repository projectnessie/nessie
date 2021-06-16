/*
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
package org.projectnessie.deltalake

import java.io.{BufferedReader, FileNotFoundException, InputStreamReader}
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.FileAlreadyExistsException
import java.util.UUID
import java.util.regex.Pattern
import org.projectnessie.client.{NessieClient, NessieConfigConstants}
import org.projectnessie.error.{
  NessieConflictException,
  NessieNotFoundException
}
import org.projectnessie.model.{
  CommitMeta,
  ContentsKey,
  DeltaLakeTable,
  ImmutableDeltaLakeTable,
  ImmutableOperations,
  Reference
}
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.storage.LogStore
import org.apache.spark.sql.delta.util.FileNames.{
  checkpointFileSingular,
  checkpointFileWithParts,
  deltaFile,
  getFileVersion
}
import org.apache.spark.sql.delta.{
  CheckpointMetaData,
  DeltaFileType,
  LogFileMeta
}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.projectnessie.model.Operation.Put

import java.util
import scala.collection.JavaConverters._
import scala.util.Try

class NessieLogStore(sparkConf: SparkConf, hadoopConf: Configuration)
    extends LogStore
    with Logging {

  val deltaFilePattern: Pattern = "\\d+-[0-9a-f]+\\.json".r.pattern
  val checksumFilePattern: Pattern = "\\d+-[0-9a-f]+\\.crc".r.pattern
  val checkpointFilePattern: Pattern =
    "\\d+-[0-9a-f]+\\.checkpoint(\\.\\d+\\.\\d+)?\\.parquet".r.pattern

  var lastSnapshotUuid: Option[String] = None

  private val client: NessieClient = {
    val catalogConfMap = catalogConf()
    val removePrefix = (x: String) => x.replace("nessie.", "")
    NessieClient
      .builder()
      .fromConfig(c => catalogConfMap.getOrElse(removePrefix(c), null))
      .build()
  }

  private def catalogName(): String = {
    CatalogNameUtil.catalogName()
  }

  private def prefix(): String = {
    s"spark.sql.catalog.${catalogName()}."
  }

  private def catalogConf(): Map[String, String] = {
    SparkSession.active.sparkContext.getConf
      .getAllWithPrefix(prefix())
      .toMap
  }

  /**
    * Keeps a mapping of reference name to current hash.
    */
  private val referenceMap: util.Map[String, Reference] = {
    val requestedRef =
      SparkSession.active.sparkContext.getConf.get(s"${prefix()}ref")

    try {
      val ref = Option(requestedRef)
        .map(client.getTreeApi.getReferenceByName(_))
        .getOrElse(client.getTreeApi.getDefaultBranch)
      val map: util.Map[String, Reference] = new util.HashMap[String, Reference]
      map.put(requestedRef, ref)
      map
    } catch {
      case ex: NessieNotFoundException =>
        if (requestedRef != null)
          throw new IllegalArgumentException(
            s"Nessie ref $requestedRef provided " +
              s"via ${NessieConfigConstants.CONF_NESSIE_REF} does not exist. This ref must exist before creating a NessieCatalog.",
            ex
          )
        throw new IllegalArgumentException(
          s"Nessie does not have an existing default branch. Either configure " +
            s"an alternative ref via ${NessieConfigConstants.CONF_NESSIE_REF} or create the default branch on the server.",
          ex
        )
    }
  }

  private def configuredRef(): Reference = {
    val refName =
      SparkSession.active.sparkContext.getConf.get(s"${prefix()}ref")
    referenceByName(refName)
  }

  private def referenceByName(refName: String): Reference = {
    var ref = referenceMap.get(refName)
    if (ref == null) {
      ref = client.getTreeApi.getReferenceByName(refName)
      referenceMap.put(refName, ref)
    }
    ref
  }

  private def updateReference(ref: Reference): Unit = {
    referenceMap.put(ref.getName, ref)
  }

  private def refreshReference(refName: String): Unit =
    referenceMap.remove(refName)

  override def listFrom(path: Path): Iterator[FileStatus] = {
    throw new UnsupportedOperationException(
      "listFrom from Nessie does not work."
    )
  }

  private def parseTableIdentifier(path: String): (String, String, String) = {
    if (path.contains("@") && path.contains("#")) {
      val tableRef = path.split("@")
      val refHash = tableRef(1).split("#")
      return (tableRef(0), refHash(0), refHash(0))
    }

    if (path.contains("@")) {
      val tableRef = path.split("@")
      return (tableRef(0), tableRef(1), null)
    }

    (
      path,
      SparkSession.active.sparkContext.getConf.get(s"${prefix()}ref"),
      SparkSession.active.sparkContext.getConf.get(s"${prefix()}hash", null)
    )
  }

  override def write(
      path: Path,
      actions: Iterator[String],
      overwrite: Boolean = false
  ): Unit = {
    if (path.getName.equals("_last_checkpoint")) {
      commit(
        path,
        configuredRef().getName,
        configuredRef().getHash,
        lastCheckpoint = actions.mkString
      )
      return
    }
    val parent = path.getParent
    val nameSplit = path.getName.split("\\.", 2)
    val (tableName, ref, hash) = parseTableIdentifier(nameSplit(0))
    val name =
      s"$tableName-${UUID.randomUUID().toString.replace("-", "")}.${nameSplit(1)}"
    val nessiePath = new Path(parent, name)

    if (overwrite)
      throw new IllegalStateException(s"Nessie won't overwrite for path $path")
    writeInternal(nessiePath, actions, ref, hash)
  }

  private def updateDeltaTable(
      path: Path,
      targetRef: String,
      lastCheckpoint: String = null
  ): DeltaLakeTable = {
    val currentTable = getTable(path.getParent, targetRef)
    val table = currentTable
      .map(ImmutableDeltaLakeTable.copyOf)
      .getOrElse(ImmutableDeltaLakeTable.builder().build())
    getFileType(path) match {
      case DeltaFileType.DELTA =>
        table.withMetadataLocationHistory(
          (path.toString :: table.getMetadataLocationHistory.asScala.toList).asJava
        )
      case DeltaFileType.CHECKPOINT =>
        throw new UnsupportedOperationException(
          "Can't write checkpoints from LogStore"
        )
      case DeltaFileType.CHECKSUM =>
        table
      case DeltaFileType.UNKNOWN =>
        if (!path.getName.equals("_last_checkpoint")) table
        else {
          val (version, parts) =
            extractCheckpoint(lastCheckpoint, path.getParent)
          table
            .withCheckpointLocationHistory(parts.asJava)
            .withMetadataLocationHistory(
              table.getCheckpointLocationHistory.asScala
                .filter(x => extractVersion(new Path(x)) < version)
                .asJava
            )
            .withLastCheckpoint(lastCheckpoint)
        }
      case _ =>
        table
    }
  }

  private def extractCheckpoint(
      lastCheckpoint: String,
      path: Path
  ): (Long, Seq[String]) = {
    implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats
    val checkpoint = parse(lastCheckpoint).extract[CheckpointMetaData]
    val version: Long = checkpoint.version
    val parts: Option[Int] = checkpoint.parts
    val tempPath = new Path(
      path,
      lastSnapshotUuid.getOrElse(
        throw new IllegalStateException(
          "didn't write the correct checkpoint dir"
        )
      )
    )
    val files =
      if (parts.isEmpty) Seq(checkpointFileSingular(tempPath, version))
      else checkpointFileWithParts(tempPath, version, checkpoint.parts.get)
    (version, moveCheckpointFiles(files, path))
  }

  private def moveCheckpointFiles(
      files: Seq[Path],
      destDir: Path
  ): Seq[String] = {
    val movedFiles = files.map(f => {
      val fs = f.getFileSystem(hadoopConf)
      val parts = f.getName.split("\\.", 2)
      val destFile =
        s"${parts(0)}-${UUID.randomUUID().toString.replace("-", "")}.${parts(1)}"
      val dest = new Path(destDir, destFile)
      fs.rename(f, dest)
      dest.toString
    })
    files.head.getFileSystem(hadoopConf).delete(files.head.getParent, true)
    movedFiles
  }

  private def commit(
      path: Path,
      ref: String,
      hash: String,
      message: String = "delta commit",
      lastCheckpoint: String = null
  ): Boolean = {
    // if no expected-hash is given and the commit runs into a conflict, let the operation retry once
    var retries = if (hash == null) 1 else 0
    while (true) {
      val targetRef = if (ref == null) configuredRef().getName else ref
      val targetHash =
        if (hash == null) referenceByName(targetRef).getHash else hash
      val table = updateDeltaTable(path, targetRef, lastCheckpoint)
      val put = Put.of(pathToKey(path.getParent), table)
      val meta = CommitMeta
        .builder()
        .message(message)
        .putProperties("spark.app.id", sparkConf.get("spark.app.id"))
        .putProperties("application.type", "delta")
        .build()
      val op = ImmutableOperations
        .builder()
        .addOperations(put)
        .commitMeta(meta)
        .build()
      try {
        val updated: Reference =
          client.getTreeApi.commitMultipleOperations(targetRef, targetHash, op)
        updateReference(
          if (updated != null) updated
          else client.getTreeApi.getReferenceByName(targetRef)
        )
        return true
      } catch {
        case ex: NessieConflictException =>
          refreshReference(targetRef)
          if (retries <= 0) {
            throw ex
          }
          retries = retries - 1
      }
    }
    true
  }

  protected def writeInternal(
      path: Path,
      actions: Iterator[String],
      ref: String,
      hash: String
  ): Unit = {
    val fs = path.getFileSystem(hadoopConf)

    if (!fs.exists(path.getParent)) {
      throw new FileNotFoundException(
        s"No such file or directory: ${path.getParent}"
      )
    }

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
        commitDone = commit(path, ref, hash)
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

  def pathToKey(path: Path): ContentsKey = {
    pathToKey(path.toUri.getPath)
  }

  def pathToKey(path: String): ContentsKey = {
    val parts = path.split("/").toList
    ContentsKey.of(parts.asJava)
  }

  def numCheckpointParts(path: Path): Option[Int] = {
    val segments = path.getName.split("\\.")

    segments match {
      case x: Array[String] if x.length == 6 => Some(segments(4).toInt)
      case _                                 => None
    }
  }

  def getFileType(path: Path): DeltaFileType = {
    path match {
      case _ if checkpointFilePattern.matcher(path.getName).matches() =>
        DeltaFileType.CHECKPOINT
      case _ if deltaFilePattern.matcher(path.getName).matches() =>
        DeltaFileType.DELTA
      case _ if checksumFilePattern.matcher(path.getName).matches() =>
        DeltaFileType.CHECKSUM
      case _ => DeltaFileType.UNKNOWN
    }
  }

  def extractMeta(fileStatus: FileStatus): LogFileMeta = {
    LogFileMeta(
      fileStatus,
      extractVersion(fileStatus.getPath),
      getFileType(fileStatus.getPath),
      numCheckpointParts(fileStatus.getPath)
    )
  }

  def extractVersion(path: Path): Long = {
    getFileType(path) match {
      case DeltaFileType.DELTA =>
        path.getName.stripSuffix(".json").split("-")(0).toLong
      case DeltaFileType.CHECKPOINT =>
        path.getName.split("\\.")(0).split("-")(0).toLong
      case DeltaFileType.CHECKSUM =>
        path.getName.stripSuffix(".crc").split("-")(0).toLong
      case _ => -1L
    }
  }

  def listFilesFrom(path: Path): Iterator[LogFileMeta] = {
    val fs = path.getFileSystem(hadoopConf)
    if (!fs.exists(path.getParent)) {
      throw new FileNotFoundException(
        s"No such file or directory: ${path.getParent}"
      )
    }

    val (tableName, ref, hash) = parseTableIdentifier(path.toUri.getPath)
    var name: String = null
    if (hash != null) {
      name = hash
    } else if (ref != null) {
      name = ref
    } else {
      name = configuredRef().getName
    }

    val currentTable = getTable(new Path(tableName).getParent, name)
    val currentMetadataPath = currentTable
      .map(_.getMetadataLocationHistory.asScala.map(new Path(_)).toSet)
      .getOrElse(throw new FileNotFoundException(s"No such table: $path"))
    val currentPath =
      if (currentTable.get.getCheckpointLocationHistory != null)
        currentMetadataPath ++ currentTable.get.getCheckpointLocationHistory.asScala
          .map(new Path(_))
      else
        currentMetadataPath
    val requestedVersion = Try(getFileVersion(path))
      .getOrElse(path.getName.stripSuffix(".checkpoint").toLong)
    val files = fs.listStatus(path.getParent)
    val filteredFiles = files
      .map(extractMeta)
      .filter(x => currentPath.contains(x.fileStatus.getPath))
      .filter(_.version >= requestedVersion)
      .sortBy(_.version)
    val maxExpected: Option[Long] =
      if (currentPath.nonEmpty) Some(currentPath.map(extractVersion).max)
      else None
    val maxFound: Option[Long] =
      if (filteredFiles.nonEmpty) Some(filteredFiles.map(_.version).max)
      else None
    require(
      maxFound.getOrElse(0L) == maxExpected.getOrElse(0L),
      s"maxFound(${maxFound.getOrElse(0L)}) != maxExpected(${maxExpected.getOrElse(0L)})"
    )
    if (filteredFiles
          .map(_.fileType)
          .count(_ == DeltaFileType.CHECKPOINT) == filteredFiles.length) {
      (filteredFiles ++ Seq(
        emptyCheckpoint(requestedVersion, filteredFiles.head)
      )).iterator
    } else filteredFiles.iterator

  }

  //this is annoying, Delta requires at least one delta file even though everything is contained in the checkpoint.
  //We add on a useless delta file here if and only if only checkpoints are required
  private def emptyCheckpoint(
      version: Long,
      logFileMeta: LogFileMeta
  ): LogFileMeta = {
    val fileStatus = new FileStatus(
      0,
      false,
      0,
      0,
      logFileMeta.fileStatus.getModificationTime,
      deltaFile(logFileMeta.fileStatus.getPath.getParent, version)
    )
    new LogFileMeta(fileStatus, version, DeltaFileType.DELTA, None)
  }

  private def getTable(path: Path, branch: String): Option[DeltaLakeTable] = {
    Try(client.getContentsApi.getContents(pathToKey(path), branch))
      .filter(x => x != null && x.isInstanceOf[DeltaLakeTable])
      .map(_.asInstanceOf[DeltaLakeTable])
      .toOption
  }

  override def read(path: Path): Seq[String] = {
    if (path.getName.equals("_last_checkpoint")) {
      val table = getTable(path.getParent, configuredRef().getName)
      val data = table
        .map(_.getLastCheckpoint)
        .getOrElse(throw new FileNotFoundException())
      if (data == null) throw new FileNotFoundException()
      Seq(data)
    } else {
      val fs = path.getFileSystem(hadoopConf)
      val stream = fs.open(path)
      try {
        val reader = new BufferedReader(new InputStreamReader(stream, UTF_8))
        IOUtils.readLines(reader).asScala.map(_.trim)
      } finally {
        stream.close()
      }
    }
  }

  override def invalidateCache(): Unit = {}

  override def isPartialWriteVisible(path: Path): Boolean = true

  override def resolveCheckpointPath(path: Path): Path = {
    lastSnapshotUuid = Some(UUID.randomUUID().toString)
    path
      .getFileSystem(hadoopConf)
      .makeQualified(new Path(path, lastSnapshotUuid.get))
  }
}
