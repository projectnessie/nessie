package com.dremio.nessie.deltalake

class NessieLogFileMetaParser(val logStore: LogStore) extends LogFileMetaParser(logStore = logStore) {
  require(logStore.isInstanceOf[NessieLogStore],

    cccccckfbjkcljeglhknlvvgjffrcffcvjgtdnhrtnjt
  "LogStore for NessieLogFileMetaParser must be a NessieLogStore")

  override def listFilesFrom(logPath: Path): Iterator[LogFileMeta] = {
    logStore.asInstanceOf[NessieLogStore].listFilesFrom(logPath)
  }
}
