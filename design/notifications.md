# Notification
This document outlines a proposal for a notification SPI in Nessie. 

## Events in scope
Every time a user:
- Creates a reference (branch or tag)
- Deletes a reference (branch or tag)
- Commits
- Assigns reference to hash (force push)

We will fire a notification. Each notification is associated with a particular event. We choose not to include content specific events (UpdateContent or DeleteContent) because these should happen at commit time. Instead a user will need to look into the list of operations performed during a commit event to filter for the event they are looking for. However, we support this use case through the use of subscriptions.

## Notification SPI
Nessie events are sent to a common "listener" implementation. Every time someone commits, creates a reference, deletes a reference or assigns a reference the `notifyEvent` method is called. Anyone that wishes to get notifications about a nessie event must implement the `NessieEventListener` interface. 

```java
/**
 * Marker interface for all nessie events.
 */
interface NessieEvent {
}

/**
 * A Nessie event listener is notified every time there's a nessie event.
 * This is called mainly by the TreeApi implementation.
 */
interface NessieEventListener {
  void notifyEvent(NessieEvent event);
}

/**
 * Notification fired every time a reference is created.
 */
interface ReferenceCreated extends NessieEvent {
  /**
   * The created reference.
   */
  Reference getReference();
}

/**
 * Notification fired every time a reference is deleted.
 */
interface ReferenceDeleted extends NessieEvent {
  /**
   * The deleted reference.
   */
  Reference getReference();
}

interface ReferenceAssigned extends NessieEvent {
  /**
   * The old reference including the old hash and ref name.
   */
  Reference getOldReference();

  /**
   * The new reference including the new hash and new ref name.
   */
  Reference getNewReference();
}

/**
 * Notification fired every time a commit happens.
 */
interface CommitEvent extends NessieEvent {
  /**
   * The reference being committed against.
   * Includes the old hash.
   */
  Reference getReference();

  /**
   * The commit metadata.
   */
  CommitMeta getMetadata();

  /**
   * The hash of the recent commit.
   */
  String getNewHash();

  /**
   * List of operations being committed.
   */
  List<Operation> getOperations();
}


```
## Subscriptions
One special EventListener is the `SubscriptionEventListener`. This will be the default implementation for nessie. The `SubscriptionEventListener` implements the `NessieEventListener` and extends it with a `subscribe` method, which signature is as follows.

```java
void subscribe(Predicate<NessieEvent> predicate, NessieEventListener listener);
```

Every time an event is fired, the SubscriptionEventListener will run the predicates that are stored and delegate to the set of listeners that are "subscribed".

## Webhooks

See [Webhooks OpenAPI Spec](webhooks-openapi.yml)