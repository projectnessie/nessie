---
search:
  exclude: true
---
<!--start-->

| Property | Description |
|----------|-------------|
| `nessie.authentication.oauth2.default-access-token-lifespan` | Default access token lifespan; if the OAuth2 server returns an access token without specifying  its expiration time, this value will be used.  <br><br>Optional, defaults to "PT1M". Must be a valid [ISO-8601 duration](https://en.wikipedia.org/wiki/ISO_8601#Durations). |
| `nessie.authentication.oauth2.default-refresh-token-lifespan` | Default refresh token lifespan. If the OAuth2 server returns a refresh token without specifying  its expiration time, this value will be used.   <br><br>Optional, defaults to "PT30M". Must be a valid [ISO-8601 duration](https://en.wikipedia.org/wiki/ISO_8601#Durations). |
| `nessie.authentication.oauth2.refresh-safety-window` | Refresh safety window to use; a new token will be fetched when the current token's remaining  lifespan is less than this value.  Optional, defaults to "PT10S". Must be a valid [ISO-8601 duration](https://en.wikipedia.org/wiki/ISO_8601#Durations). |
| `nessie.authentication.oauth2.preemptive-token-refresh-idle-timeout` | Defines for how long the OAuth2 provider should keep the tokens fresh, if the client is not  being actively used.  Setting this value too high may cause an excessive usage of network I/O  and thread resources; conversely, when setting it too low, if the client is used again, the  calling thread may block if the tokens are expired and need to be renewed synchronously.  Optional, defaults to "PT30S". Must be a valid  [ISO-8601 duration](https://en.wikipedia.org/wiki/ISO_8601#Durations). |
| `nessie.authentication.oauth2.background-thread-idle-timeout` | Defines how long the background thread should be kept running if the client is not being  actively used, or no token refreshes are being executed.  Optional, defaults to "PT30S". Setting this value too high will cause the background  thread to keep running even if the client is not used anymore, potentially leaking thread and  memory resources; conversely, setting it too low could cause the background thread to be  restarted too often. Must be a valid [ISO-8601 duration](https://en.wikipedia.org/wiki/ISO_8601#Durations). |