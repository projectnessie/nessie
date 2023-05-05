# Nessie Events Service

This module contains the service layer of the Nessie events notification system. This layer is
agnostic of any specific platform or application framework.

The main class is `org.projectnessie.events.service.EventService`. It takes care of starting and
stopping the subscribers, and provides the machinery for firing events and delivering them to
subscribers, with support for retries and backoff. `EventService` is thread-safe and is meant to
be used as a singleton, or in CDI application scope.

Another important class is `org.projectnessie.events.service.ResultCollector`. Instances of this 
class are meant to be injected into version stores to collect results. They are meant to be 
request-scoped, in order to capture the user principal that initiated the request, and the 
repository id.
