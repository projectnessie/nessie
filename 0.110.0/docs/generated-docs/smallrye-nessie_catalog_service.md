---
search:
  exclude: true
---
<!--start-->

| Property | Default Value | Type | Description |
|----------|---------------|------|-------------|
| `nessie.catalog.service.imports.max-concurrent` | `32` | `int` | Advanced property, defines the maximum number of concurrent imports from object stores.  |
| `nessie.catalog.service.tasks.threads.max` | `-1` | `int` | Advanced property, defines the maximum number of threads for async tasks like imports.  |
| `nessie.catalog.service.tasks.threads.keep-alive` | `PT2S` | `duration` | Advanced thread pool setting for async tasks like imports.  |
| `nessie.catalog.service.tasks.minimum-delay` | `PT0.001S` | `duration` | Advanced thread pool setting for async tasks like imports.  |
| `nessie.catalog.service.race.wait.min` | `PT0.005S` | `duration` | Advanced thread pool setting for async tasks like imports.  |
| `nessie.catalog.service.race.wait.max` | `PT0.250S` | `duration` | Advanced thread pool setting for async tasks like imports.  |
