package com.dremio.nessie.deltalake
package com.dremio.nessie.deltalake

import java.io.FileNotFoundException

class NessieLogStore(sparkConf: SparkConf,
                     hadoopConf: Configuration) extends LogStore with Logging {

  override def read(path: Path): Seq[String] = ???

  override def write(path: Path, actions: Iterator[String], overwrite: Boolean): Unit = ???

  override def listFrom(path: Path): Iterator[FileStatus] = {
    /* here we need to:
      a) extract table name from path
      b) check table name exists in nessie
      c) pull current RPS from store
      d) build the list of files
      e) sort
      f) trim the ones older than RPS
      g) ensure there are no gaps...means inconsistent and we should probably wait
     */

    val fs = path.getFileSystem(hadoopConf)
    if (!fs.exists(path.getParent)) {
      throw new FileNotFoundException(s"No such file or directory: ${path.getParent}")
    }
    val files = fs.listStatus(path.getParent)
    files.map(makePair)
      .filter(_._2 >= path.getName).sortBy(_._2).map(_._1).iterator

  }

  def makePair(path: FileStatus): (FileStatus, String) = {
    (path, stripPrefix(path.getPath))
  }

  def stripPrefix(path: Path): String = {
    val parts = path.getName.split("\\.", 2)
    if (Try(parts(0).toLong).isFailure) parts(1) else path.getName
  }

  override def invalidateCache(): Unit = {
  }

  def listFilesFrom(logPath: Path): Iterator[LogFileMeta] = {

  }
}
