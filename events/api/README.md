# Nessie Events API

This module contains the API for the Nessie events notification system.

## Overview

The `org.projectnessie.events.api` package contains the API classes for the Nessie events
notification system.

The main entry point of the API is the `Event` interface. Instances of `Event` are created and
published by the Nessie server and consumed by registered event subscribers.

The `Event` interface has a `getType()` method that returns the type of the event. The type is an
enum that uniquely identifies the event type. This enum serves as a discriminator for the
event type and allows subscribers to filter events by type.

The following concrete event implementations are defined:

* `CommitEvent`: This event is published when a commit is performed.
* `MergeEvent`: This event is published when a merge is performed.
* `TransplantEvent`: This event is published when a transplant is performed.
* `ReferenceCreatedEvent`: This event is published when a reference is created.
* `ReferenceUpdatedEvent`: This event is published when a reference is updated.
* `ReferenceDeletedEvent`: This event is published when a reference is deleted.
* `ContentStoredEvent`: This event is published when a content is stored (PUT).
* `ContentRemovedEvent`: This event is published when a content is removed (DELETE).
