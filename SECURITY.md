# Security Policy

Project Nessie's runtime-server security assumptions and trust boundaries are documented in
[SECURITY_THREAT_MODEL.md](SECURITY_THREAT_MODEL.md). Use that document to understand the intended
security contract for authentication, authorization, native Nessie REST APIs, Iceberg REST APIs,
catalog/object-store access, and deployment assumptions.

## Supported Versions

Currently supported versions are listed below.

| Version  | Supported          |
|----------|--------------------|
| 0.108.2   | :white_check_mark: |
| < 0.108.2 | :x:                |

All Nessie 0.x.x versions are considered beta or even alpha releases and not supported after
release of Nessie 1.0.0.

## Reporting a Vulnerability

Any security issues should be reported to security@projectnessie.org, please refrain from posting publicly until the team can investigate and patch the code.
