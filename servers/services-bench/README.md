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

See the [Async Profiler repo](https://github.com/async-profiler/async-profiler) for a pre-built library or how to build it from source and also how to add it to a
JMH run. The following examples worked at the time of writing this README.

### Running on Linux

```bash
ASYNC_PROFILER_DLL=(path to async profiler libararies)
LD_LIBRARY_PATH=${ASYNC_PROFILER_DLL} java \
  -jar servers/services-bench/build/libs/nessie-services-bench-*-jmh.jar \
  -prof async
```

### Running on macOS

```bash
ASYNC_PROFILER_DLL=(path to async profiler libararies)
java \
  -Djava.library.path=${ASYNC_PROFILER_DLL} \
  -jar servers/services-bench/build/libs/nessie-services-bench-*-jmh.jar \
  -prof async
```

## Linux Perf tools

Install the appropriate package for your distribution (`linux-tools` on Ubuntu), or `make` it from the Linux sources
in `tools/perf` for your running Linux kernel version. `perf` needs to be in `PATH`.
