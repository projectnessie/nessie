
# Notifications
This document outlines the design for notifications in Nessie with an application for Webhooks. It outlines the different
events that we support, the way the notifications are called, how they are stored and what is the API used for creating,
listing, updating and deleting notifications for a nessie server.

## Event types
This section lists out all events we support. All events in Nessie contain two pieces of information: the event time and
the event type. The event time is the time at which the event was fired, it corresponds closely to when the event
happened, but it can lag behind a few nanoseconds. The event type is an `UPPER_CASE` string representing the type of the
event, as an example, commit events have a `"COMMIT"` type.

For the following section, we will use the same schema as Nessie's API for referring to references, branches and tags,
as well, as other Nessie API model objects, such as CommitMeta and Operation. 

We will refer to them as their type in the Nessie OpenAPI spec, using the `$ref` keyword. 

### Commits
Commit events are triggered whenever there's a commit to a branch, i.e. whenever the `/trees/branch/{branchName}/commit`
API in Nessie is called. 

#### Schema
```yaml
CommitEvent:
  type: object
  properties:
    type:
      type: string
      description: Always COMMIT
    eventTime:
      type: string
      format: date
      description: The time the event is fired in ISO date format.
    branch:
      $ref: Branch
      description: The branch committed against.
    newHash:
      type : string
      description: The hash of this commit.
    metadata:
      $ref: CommitMeta
      description: This commit's metadata. Containing author, committer, etc.
    operations:
      type: array
      items:
        $ref: 'Operation'
```

#### Example
```json
{
  "type" : "COMMIT",
  "eventTime": "2009-02-14T18:09:10Z",
  "reference": {
    "type" : "BRANCH",
    "name" : "main",
    "hash" : "6a3efd4f40e88a36"
  },
  "metadata": {
    "author" : "Bob",
    "message" : "Delete tables.bobby",
    "commitTime" : "2015-04-14T22:45:00Z"
  },
  "newHash" : "e2dad0c9178ef5ff",
  "operations": [
    {"type":"DELETE","key":{"elements":["tables","bobby"]}}
  ]
}
```

### Merges
Merge events are triggered whenever we merge a reference into a branch. In other words, whenever the 
`/trees/branch/{branchName}/merge` API in Nessie is called.

#### Schema
```yaml
MergeEvent:
  type: object
  properties:
    type:
      type: string
      description: Always "MERGE"
    eventTime:
      type: string
      format: date
      description: The time the event was fired in ISO date format.
    fromRefName:
      type: string
      description: The name of the reference being merged into.
    fromHash:
      type: string
      description: The hash to merge from.
    toBranchName:
      type: string
      description: The name of the branch to merge into.
    expectedHash:
      type: string
      description: The hash at which the branch was at the moment of merge.
    newHash:
      type: string
      description: The hash of the merge commit.
```

#### Example
```json
{
  "type": "MERGE",
  "eventTime": "2009-02-14T18:09:10Z",
  "fromRefName" : "dev",
  "fromHash" : "e2dad0c9178ef5ff",
  "toBranchName": "main",
  "expectedHash" : "6a3efd4f40e88a36",
  "newHash": "ffaabb78792ddd88"
}
```

### Transplants
Transplants events are triggered whenever transplants from one branch into another happens, i.e. whenever the 
`/trees/branch/{branchName}/transplant` API in Nessie is called.

#### Schema
```yaml
TransplantEvent:
  type: object
  properties:
    type:
      type: string
      description: Always "TRANSPLANT"
    eventTime:
      type: string
      format: date
      description: The time the event was fired in ISO date format.
    fromRefName:
      type: string
      description: The name of the references to transplant commits from.
    fromHashes:
      type: array
      description: The hashes of the commits to transplant
      items:
        type: string
    toBranchName:
      type: string
      description: The name of the branch to transplant commits into.
    expectedHash:
      type: string
      description: The hash at which the branch was expected to be in at the moment of the transplant.
    newHash:
      type: string
      description: The hash of the head of this branch after the transplant.

```

#### Example
```json
{
  "type": "TRANSPLANT",
  "eventTime": "2009-02-14T18:09:10Z",
  "fromRefName": "dev",
  "fromHashes": ["d440310c007bb303", "46d1ec0e7680b0ed", "018eb26ce9eb4712"],
  "toBranchName": "main",
  "expectedHash": "85a1090b5fe01a7b",
  "newHash": "01f53acc6fdd6134"
}
```

### Reference creation
Reference creation events are triggered whenever a reference is created. In other words, whenever the 
`POST /trees/tree/` API in Nessie is called.

#### Schema
```yaml
ReferenceCreated:
  type: object
  properties:
    type:
      type: string
      description: Always REFERENCE_CREATED
    eventTime:
      type: string
      format: date
      description: The time the event is fired in ISO date format.
    reference:
      $ref: Reference
      description: The recently created reference.
```

#### Example
```json
{
  "type": "REFERENCE_CREATED",
  "eventTime": "2009-02-14T18:09:10Z",
  "reference": {
    "type":"BRANCH",
    "name":"dev",
    "hash":"e249ff3a681f177c"
  }
}
```

### Reference assignment
Reference assignment events are triggered whenever a reference is assigned. In other words, whenever the 
`PUT /trees/{referenceType}/{referenceName}` API in Nessie is called.

#### Schema
```yaml
ReferenceAssigned:
  type: object
  properties:
    type:
      type: string
      description: Always REFERENCE_ASSIGNED
    eventTime:
      type: string
        format: date
        description: The time the event is fired in ISO date format.
    reference:
      $ref: Reference
      description: The reassigned reference.
    assignedTo:
      $ref: Reference 
      description: The reference to which the reference got assigned to.
```

#### Example
```json
{
  "type": "REFERENCE_ASSIGNED",
  "eventTime": "2009-02-14T18:09:10Z",
  "reference": {
    "type": "BRANCH",
    "name": "dev",
    "hash": "e249ff3a681f177c"
  },
  "assignedTo": {
    "type": "TAG",
    "name": "release-10",
    "hash": "e249ff3a681f177c"
  }
}
```

### Reference deletion
Reference deletion events are triggered whenever a reference is deleted. In other words,  whenever the 
`DELETE /trees/{referenceType}/{referenceName}` API in Nessie is called.

#### Schema
```yaml
ReferenceDeleted:
  type: object
  properties:
    type:
      type: string
      description: Always REFERENCE_DELETED
    eventTime:
      type: string
      format: date
      description: The time the event is fired in ISO date format.
    referenceType:
      type: string
      description: The reference type. Either BRANCH or TAG.
    referenceName:
      type: string
      description: The deleted reference name.
```

#### Example
```json
{
  "type": "REFERENCE_DELETED",
  "eventTime": "2009-02-14T18:09:10Z",
  "referenceType": "BRANCH",
  "referenceName": "main"
}
```

## Webhooks/Notifications API
A webhook is a method to augment the behavior of a web service with custom callbacks. These callbacks are usually a URL
that a client registers with the webservice, in order for it to get invoked by the webservice whenever there's a
relevant event.

In Nessie's case, the contract between the webservice (caller) and the webhook URL (callee) is a simple HTTP request
sent to the registered webhook. The contract is as follows:
1. The client will register a webhook notification with Nessie by calling one of the APIs below.
2. When an event happens in Nessie, it will call back using a `POST` request that:
   1. Its content-type is `application/json` or a content-type ending in `+json`.
   2. Its body contains the JSON payload of the event that happened. (See previous section for details on the expected schema).

### Creating a new notification/webhook
There are different event types a client might want to get notified about. In order to subscribe to a particular event 
type, a client needs to make a `POST` request to one or more of the following endpoints.

* `/notifications/commits`
* `/notifications/merges`
* `/notifications/transplants`
* `/notifications/references-created`
* `/notifications/references-deleted`
* `/notifications/references-assigned`

The request body is expected to have the following schema
```yaml
Notification:
  type: object
  properties:
    type:
      type: string
      description: The type of notification. Currently only "WEBHOOK" is supported.
    url:
      type: string
      format: uri
      description: The url to callback.
```

A webhook notification creation request might look like

```json
{
  "type": "WEBHOOK",
  "url": "http://www.example.org/webhook-callback"
}
```

Nessie will respond with a `201 Created` response and a `Location` header corresponding to the URI of the recently
created notification. This resource URI will be used for deleting, updating and describing the notification.

```
HTTP/1.1 201 Created
Location: http://nessie.dremio.site/api/v1/notifications/6bbdcc62-e5b6-4e1b-8f06-3b0bb3620d5e
Content-type: application/json

{
  "id": "6bbdcc62-e5b6-4e1b-8f06-3b0bb3620d5e",
  "type": "WEBHOOK",
  "url": "http://www.example.org/webhook-callback"
}
```

Important note is that nessie doesn't make any efforts to de-duplicate the URL and if a particular URL is subscribed
twice to the same type of event. Nessie will notify twice, once per each notification.

### Describing, updating and deleting notifications
As previously described, when creating a notification, the response will contain the URI of the newly created 
notification. This URI is then used for describing, updating and deleting the specific notification. In general, 
the following methods will be used.

* `GET /notifications/{notificationId}` for describing a notification.
* `PUT /notifications/{notificationId}` for updating a notification.
* `DELETE /notifications/{notificationId}` for deleting a notification.

## Implementation details
### Observer extension point
All the event types that we are interested are associated with the `/tree` API. We propose to extend the current 
TreeApiImpl class with an observer pattern. Every time the specific method in the TreeApi is called, an event is sent
to the observer. This extension point allows extending to other types of notifications in the future.

### Notification persistence
Because notifications need to survive between nessie service restarts. We propose to have a separate persistence layer
for notifications. This persistence layer will use the same database connection as the VersionStore, but will not use
the same code as this is rather specific to repository operations. This means that things like `DynamoDatabaseClient`, 
`MongoDatabaseClient` and `TxConnectionProvider` will be modified to have a "notifications" table/collection and be used
for a new `NotificationStore`.
