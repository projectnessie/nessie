```
2025-12-05 15:26:41,484 WARN  [org.hibernate.validator.internal.metadata.aggregated.CascadingMetaDataBuilder] (main) HV000271: Using `@Valid` on a container (java.util.List) is deprecated. You should apply the annotation on the type argument(s). Affected element: ContentService#getMultipleContents(String, String, List, boolean, RequestMeta)
2025-12-05 15:26:41,947 WARN  [org.hibernate.validator.internal.metadata.aggregated.CascadingMetaDataBuilder] (main) HV000271: Using `@Valid` on a container (java.util.List) is deprecated. You should apply the annotation on the type argument(s). Affected element: ContentService#getMultipleContents(String, String, List, boolean, RequestMeta)
Usage: nessie-server-admin-tool-runner.jar delete-catalog-tasks [-hV]
       [-B=<batchSize>] [-H=<hash>] [-r=<ref>] [-k=<keyElements>]...
       [-s=<statuses>]...
Delete persisted state of Iceberg snapshot loading tasks previously executed by
the Nessie Catalog.
  -B, --batch=<batchSize>   The max number of task IDs to process at the same
                              time.
  -h, --help                Show this help message and exit.
  -H, --hash=<hash>         Commit hash to use (defaults to the HEAD of the
                              specified reference).
  -k, --key-element=<keyElements>
                            Elements or a specific content key to process (zero
                              or more). If not set, all current keys will get
                              their snapshot tasks expired.
  -r, --ref=<ref>           Reference name to use (default branch, if not set).
  -s, --task-status=<statuses>
                            Delete tasks having these statuses (zero or more).
                              If not set, only failed tasks for matching
                              content objects are deleted.
  -V, --version             Print version information and exit.

```
