---
search:
  exclude: true
---
<!--start-->

```
Usage: nessie-server-admin-tool-runner.jar check-content [-cEhsV]
       [-B=<batchSize>] [-H=<hash>] [-o=<outputSpec>] [-r=<ref>]
       [-k=<keyElements>]...
Check content readability of active keys.
  -B, --batch=<batchSize>   The max number of keys to load at the same time.
                            If an error occurs while loading or parsing the
                              values for a single key, the error will be
                              propagated to all keys processed in the same
                              batch. In such a case, rerun the check for the
                              affected keys with a batch size of 1.
  -c, --show-content        Include content for each valid key in the output.
  -E, --error-only          Produce JSON only for keys with errors.
  -h, --help                Show this help message and exit.
  -H, --hash=<hash>         Commit hash to use (defaults to the HEAD of the
                              specified reference).
  -k, --key-element=<keyElements>
                            Elements or a specific content key to check (zero
                              or more). If not set, all current keys will be
                              checked.
  -o, --output=<outputSpec> JSON output file name or '-' for STDOUT. If not
                              set, per-key status is not reported.
  -r, --ref=<ref>           Reference name to use (default branch, if not set).
  -s, --summary             Print a summary of results to STDOUT (irrespective
                              of the --output option).
  -V, --version             Print version information and exit.

```
