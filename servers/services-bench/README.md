# Nessie VersionStore micro benchmarks

Building:
```bash
./gradlew :nessie-services-bench:jmhJar
```

Running:
```bash
java -jar servers/services-bench/build/libs/nessie-services-bench-*-jmh.jar
```

## Async-profiler

See the [Async Profiler repo](https://github.com/async-profiler/async-profiler) for a pre-built library or how to build
it from source.

Running (Linux):
```bash
LD_LIBRARY_PATH=(PATH-TO-LIBRARY)/ java \
  -jar servers/services-bench/build/libs/nessie-services-bench-*-jmh.jar \
  -prof async
```

## Linux Perf tools

Install the appropriate `linux-tools` package for your distribution, or `make` it from the Linux sources in `tools/perf`
for your running Linux kernel version. `perf` needs to be in `PATH`.
