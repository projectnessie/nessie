# Nessie Events SPI

This module contains the SPI for the Nessie events notification system.

## Overview

The `org.projectnessie.events.spi` package contains the SPI for the Nessie events notification
system.

The main entry point of the SPI is the `EventSubscriber` interface. The `EventSubscriber` interface
must be implemented by users of the notification system. Implementations of `EventSubscriber` are
discovered using Java's ServiceLoader mechanism.

The `EventSubscriber` interface has several `onXYZ()` methods that are called when an event of the
corresponding type is published. See the javadocs of each method for details.

