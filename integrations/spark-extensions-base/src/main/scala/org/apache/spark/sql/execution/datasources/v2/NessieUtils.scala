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
package org.apache.spark.sql.execution.datasources.v2

import org.projectnessie.client.api.NessieApiV1
import org.projectnessie.error.NessieReferenceNotFoundException
import org.projectnessie.model._

import java.time.format.DateTimeParseException
import java.time.{Instant, ZonedDateTime}

object NessieUtils {

  val BRANCH: String = "Branch"
  val TAG: String = "Tag"

  def unquoteRefName(branch: String): String = if (
    branch.startsWith("`") && branch.endsWith("`")
  ) {
    branch.substring(1, branch.length - 1)
  } else branch

  def calculateRef(
      branch: String,
      tsOrHash: Option[String],
      api: NessieApiV1
  ): Reference = {
    val refName = unquoteRefName(branch)

    val hash = tsOrHash
      .map(x => x.replaceAll("`", ""))
      .filter(x => Validation.isValidHash(x))
      .orNull

    if (null != hash) {
      return findReferenceFromHash(refName, hash, api)
    }

    val timestamp = tsOrHash
      .map(x => x.replaceAll("`", ""))
      .map(x => {
        try {
          ZonedDateTime.parse(x).toInstant
        } catch {
          case e: DateTimeParseException =>
            throw new NessieReferenceNotFoundException(
              String.format(
                "Invalid timestamp provided: %s. You need to provide it with a zone info. For more info, see: https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html",
                e.getMessage
              )
            )
        }
      })
      .orNull

    if (timestamp == null) {
      api.getReference.refName(refName).get
    } else {
      findReferenceFromTimestamp(refName, api, timestamp)
    }
  }

  private def findReferenceFromHash(
      branch: String,
      requestedHash: String,
      api: NessieApiV1
  ) = {
    val commit = Option(
      api.getCommitLog
        .refName(branch)
        .hashOnRef(Validation.validateHash(requestedHash))
        .stream()
        .findFirst()
        .orElse(null)
    ).map(x => x.getCommitMeta.getHash)
    val hash = commit match {
      case Some(value) => value
      case None =>
        throw new NessieReferenceNotFoundException(
          String.format(
            "Cannot find requested hash %s on reference %s.",
            requestedHash,
            branch
          )
        )
    }
    convertToSpecificRef(
      hash,
      api.getReference.refName(branch).get()
    )
  }

  private def findReferenceFromTimestamp(
      branch: String,
      api: NessieApiV1,
      timestamp: Instant
  ) = {
    val commit = Option(
      api.getCommitLog
        .refName(branch)
        .filter(
          String.format(
            "timestamp(commit.commitTime) <= timestamp('%s')",
            timestamp
          )
        )
        .stream()
        .findFirst()
        .orElse(null)
    ).map(x => x.getCommitMeta.getHash)

    val hash = commit match {
      case Some(value) => value
      case None =>
        throw new NessieReferenceNotFoundException(
          String.format("Cannot find a hash before %s.", timestamp)
        )
    }
    convertToSpecificRef(
      hash,
      api.getReference.refName(branch).get()
    )
  }

  private def convertToSpecificRef(hash: String, reference: Reference) = {
    reference match {
      case branch: ImmutableBranch => Branch.of(branch.getName, hash)
      case tag: ImmutableTag       => Tag.of(tag.getName, hash)
      case _ =>
        throw new UnsupportedOperationException(
          s"Unknown reference type $reference"
        )
    }
  }

  def getRefType(ref: Reference): String = {
    ref match {
      case _: ImmutableBranch => NessieUtils.BRANCH
      case _: ImmutableTag    => NessieUtils.TAG
      case _ =>
        throw new UnsupportedOperationException(s"Unknown reference type $ref")
    }
  }
}
