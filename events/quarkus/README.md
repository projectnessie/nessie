# Nessie Events Quarkus

This module contains the Quarkus-specific implementation of the Nessie events notification system.

To improve isolation and facilitate testing, this module is completely independent of other 
Quarkus modules; it contains:

1. Quarkus-specific implementations of nessie-events-service classes;
2. Asynchronous delivery based on Vert.x, both non-blocking and blocking;
3. Delivery with optional logging, tracing and metrics.
