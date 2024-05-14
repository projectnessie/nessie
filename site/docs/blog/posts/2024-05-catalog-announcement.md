---
date: 2024-05-13
authors:
  - snazy
---

# Nessie integration of Iceberg REST

Dear Project Nessie and Apache Iceberg communities,

We’re proud to [announce](https://www.youtube.com/watch?v=Bkpj7M6yVdQ) that we will soon integrate
the [Apache Iceberg REST spec](https://iceberg.apache.org/concepts/catalog/#decoupling-using-the-rest-catalog) into
open-source Nessie! With this integration, you’ll be able to use any client that supports the Iceberg REST spec with
Nessie. We plan to roll out this new capability in the next few weeks.
<!-- more -->

By integrating the Iceberg REST spec into Nessie, clients will work interchangeably with
the [Iceberg REST](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml) catalog and
the [Nessie catalog](https://iceberg.apache.org/docs/1.5.0/nessie/). In addition, Iceberg REST will enable Nessie to
manage access to object storage more simply and securely by supporting S3 signed requests and session credentials.
You’ll be able to easily use the Iceberg REST catalog with Nessie by configuring your Iceberg client to point to the
Nessie Iceberg REST base URI. The existing Nessie catalog in Iceberg will be unaffected by this change.

We are actively working with the Apache Iceberg community and other major adopters to improve the Iceberg REST spec, add
more features and functionality, and make it an even better option to use Nessie with Iceberg.

Please reach out to us in the [Project Nessie Zulip](https://project-nessie.zulipchat.com/) chat
or [Apache Iceberg community](https://iceberg.apache.org/community/) if you’d like to ask questions or share feedback.

Best,

The Nessie OSS team
