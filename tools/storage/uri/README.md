# Storage URI Utilities

This module contains utilities for dealing with Object Storage URIs, which in many cases do not follow
the [RFC 3986](https://www.rfc-editor.org/rfc/rfc3986) syntax.

In particular, Amazon S3 URIs permit unescaped quote characters.
