Connect to a Nessie repository using the Nessie REST API base URI specified via `Uri`.

Nessie client options can be specified using the `ParamKey = Value` pairs, see
https://projectnessie.org/nessie-latest/client_config/,

If the Nessie CLI is already connected to a Nessie repository, that connection will be closed.

Tip: If your connect string contains sensitive information, like a password or an access token,
put a space _before_ the `CONNECT` statement, which instructs the Nessie CLI to not record the
statement in the history file.

Examples:

* Local Nessie:
  `CONNECT TO http://127.0.0.1:19120/api/v2` for a locally running Nessie instance (on your
  machine) without authentication enabled.
* Dremio Cloud:
  `CONNECT to https://app.dremio.cloud/repositories/beeff00d-1122-1234-4242-feedcafebabe/api/v2
    USING "nessie.authentication.type" = BEARER
    AND "nessie.authentication.token" = "your-personal-bearer-token"`
  to connect to Dremio Cloud. (The statement must not contain line breaks.)
  Replace `beeff00d-1122-1234-4242-feedcafebabe` with your project ID. The complete endpoint
  URI of your Dremio Cloud catalog can be found in the catalog settings
  as _Catalog Endpoint_ under _General Information_.
  Also replace `your-personal-bearer-token` with a personal token, which can be created in
  your account settings under _Personal Access Tokens_. Do never share a personal access token!

When using "interactive" OAuth2 device-code or authorization-code flows, you can abort the
login by pressing Ctrl-C.
