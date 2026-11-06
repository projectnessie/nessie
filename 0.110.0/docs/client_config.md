---
title: "Client Configuration"
---

# Client Configuration

When Nessie is integrated into a broader data processing environment, authentication settings need to be provided in
a way specific to the tool used.

## Nessie client configuration options

See also [Authentication Settings](#authentication-settings) below.

### Common settings

{% include './generated-docs/client-config-main.md' %}

### Network settings

{% include './generated-docs/client-config-Network.md' %}

### HTTP settings

{% include './generated-docs/client-config-Network_HTTP.md' %}

### Bearer authentication settings

See also [Authentication Settings](#authentication-settings) below.

{% include './generated-docs/client-config-Bearer_Authentication.md' %}

### OAuth2 settings

General OAuth2 settings. See also [Authentication Settings](#authentication-settings) below.

{% include './generated-docs/client-config-OAuth2_Authentication.md' %}

#### OAuth2 Resource Owner Password Credentials settings

OAuth2 settings relevant when using the `password` grant type. See [below](#authentication-settings) for details.

{% include './generated-docs/client-config-OAuth2_Authentication_Password.md' %}

#### OAuth2 Authorization Code Grant settings

OAuth2 settings relevant when using the `authorization_code` grant type. See [below](#authentication-settings) for details.

{% include './generated-docs/client-config-OAuth2_Authentication_Authorization_Code.md' %}

#### OAuth2 Device Authorization Grant settings

OAuth2 settings relevant when using the `device_code` grant type. See [below](#authentication-settings) for details.

{% include './generated-docs/client-config-OAuth2_Authentication_Device_Code.md' %}

#### OAuth2 Token Exchange Grant settings

OAuth2 settings relevant when using the `token_exchange` grant type. See [below](#authentication-settings) for details.

!!! warning
    The feature is experimental and subject to change.

{% include './generated-docs/client-config-OAuth2_Authentication_Token_Exchange.md' %}

#### OAuth2 impersonation settings

OAuth2 settings relevant when using impersonation. See [below](#authentication-settings) for details.

!!! warning
    The feature is experimental and subject to change.

{% include './generated-docs/client-config-OAuth2_Authentication_Impersonation.md' %}

#### OAuth2 token refresh settings

OAuth2 settings related to token refreshes. You should rarely need to change the defaults.

{% include './generated-docs/client-config-OAuth2_Authentication_Token_Refresh.md' %}

### AWS authentication settings

Additional AWS authentication configuration should be provided via standard AWS configuration files.

See also [Authentication Settings](#authentication-settings) below.

{% include './generated-docs/client-config-AWS_Authentication.md' %}

### Basic authentication settings

See also [Authentication Settings](#authentication-settings) below.

{% include './generated-docs/client-config-Basic_Authentication.md' %}

### Java 11 connection pool options

The Java 11 HTTP client can be configured using Java system properties. Since Java's `HttpClient`
API does not support the configuration of these properties programmatically, Nessie cannot expose
those via its configuration mechanism.

| System property                     | Meaning                                                                                                          |
|-------------------------------------|------------------------------------------------------------------------------------------------------------------|
| `jdk.httpclient.connectionPoolSize` | The size of the HTTP connection pool.Defaults to `0`, which means the number of connections is unlimited.        |
| `jdk.httpclient.keepalive.timeout`  | Number of seconds an idle HTTP connection will be kept alive. Defaults is `1200` seconds.                        |
| `jdk.httpclient.receiveBufferSize`  | Size of the network level receive buffer size. Defaults to `0`, which means the operating system defaults apply. |
| `jdk.httpclient.sendBufferSize`     | Size of the network level send buffer size. Defaults to `0`, which means the operating system defaults apply.    |

!!! note
    See Javadoc of `javax.net.ssl.SSLParameters` for valid options/values for the configuration
    parameters starting with `nessie.ssl.`.

!!! note
    See Javadoc of `org.projectnessie.client.NessieConfigConstants` as well.

!!! note
    In case you run into issues with Nessie's new HTTP client for Java 11 and newer, you can try
    to use the legacy `URLConnection` based HTTP client by setting the system property or configuration option
    `nessie.client-builder-name` to `URLConnection`.

## Spark

When Nessie is used in Spark-based environments (with [Iceberg](../iceberg/iceberg.md) 
the Nessie authentication settings are configured via Spark session properties (Replace `<catalog_name>` with the name of your catalog).

=== "Java"
    ```java
    // local spark instance, assuming NONE authentication
    conf.set("spark.sql.catalog.<catalog_name>", "org.apache.iceberg.spark.SparkCatalog")
        .set("spark.sql.catalog.<catalog_name>.authentication.type", "NONE")
        .set(...);
    spark = SparkSession.builder()
                        .master("local[2]")
                        .config(conf)
                        .getOrCreate();
    ```
=== "Python"
    ```python
    # local spark instance, assuming NONE authentication
    spark = SparkSession.builder \
            .config("spark.sql.catalog.<catalog_name>", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.<catalog_name>.authentication.type", "NONE") \
            .config(...) 
            .getOrCreate()
    ```

### Property Prefixes

The `spark.sql.catalog.<catalog_name>` prefix identifies properties for the Nessie catalog. The `<catalog_name>` part is just
the name of the catalog in this case (not to be confused with the Nessie project name).

Multiple Nessie catalogs can be configured in the same Spark environment, each with its own
set of configuration properties and its own property name prefix.

## Flink

When Nessie is used in Flink with [Iceberg](../iceberg/iceberg.md), the Nessie authentication settings are configured when creating the Nessie catalog in Flink (Replace `<catalog_name>` with the name of your catalog):

```python
table_env.execute_sql(
        """CREATE CATALOG <catalog_name> WITH (
        'type'='iceberg',
        'catalog-impl'='org.apache.iceberg.nessie.NessieCatalog',
        'authentication.type'='NONE')""")
```

## Authentication Settings

The sections below discuss specific authentication settings. The property names are shown without
environment-specific prefixes for brevity. Nonetheless, in practice the property names should be
given appropriate prefixes (as in the examples above) for them to be recognized by the tools and
Nessie code.

The value of the `authentication.type` property can be one of the following:

* `NONE` (default)
* `BEARER`
* `OAUTH2`
* `AWS`

### Authentication Type `NONE`

For the Authentication Type `NONE` only the `authentication.type` property needs to be set.

This is also the default authentication type if nothing else is configured.

### Authentication Type `BEARER`

For the `BEARER` Authentication Type the `authentication.token` property should be set to a valid
[OpenID token](https://openid.net/specs/openid-connect-core-1_0.html).

This authentication type is recommended only when the issued access token has a lifespan large
enough to cover the duration of the entire Nessie client's session. Once the token is expired, the
Nessie client will not be able to refresh it and will have to be restarted, with a different token.
If the token needs to be refreshed periodically, then the `OAUTH2` authentication type should be
preferred to this one.

### Authentication Type `OAUTH2`

The `OAUTH2` Authentication Type is able to authenticate against an OAuth2 server and obtain a valid
access token. Only Bearer access tokens are currently supported. The access token is then used to
authenticate against Nessie. The client will automatically refresh the access token. This
authentication type is recommended when the access token has a lifespan shorter than the Nessie
client's session lifespan.

Note that the Nessie server must be configured to accept OAuth2 tokens from the same server. For
example, if the OAuth2 server is Keycloak, this can be done by defining the following properties in
the `application.properties` file of the Nessie server:

```properties
nessie.server.authentication.enabled=true
quarkus.oidc.auth-server-url=https://<keycloak-server>/realms/<realm-name>
```

OAuth is a complex framework and usually requires many configuration settings on the client side.
The full list of available settings is [shown above](#oauth2-settings), but here are some general
configuration guidelines:

#### Configuring endpoints

The Nessie client interacts with the OAuth2 server by contacting its _endpoints_, in order to
authenticate and obtain access tokens, using various _grants_. The main endpoint is the _token
endpoint_ and is always required, but other endpoints may also be required, depending on the grant
type being used. 

The endpoints can be provided with the following properties:

* Token endpoint: `nessie.authentication.oauth2.token-endpoint` (always required);
* Authorization endpoint: `nessie.authentication.oauth2.auth-endpoint` (required when using the
  `authorization_code` grant);
* Device authorization endpoint: `nessie.authentication.oauth2.device-auth-endpoint` (required when 
  using the `device_code` grant).

However, instead of specifying the endpoints individually, it is recommended to use the all-in-one
property `nessie.authentication.oauth2.issuer-url` whenever possible. When this property is
provided, the client is capable of discovering all the required endpoints automatically by querying
the authorization server well-known metadata endpoint.

#### Configuring grant types

Another important property is `authentication.oauth2.grant-type`, which defines the grant type to
use when authenticating against the OAuth2 server. Valid values are:

* `client_credentials` : enables the [Client Credentials grant] (default);
* `password` : enables the [Resource Owner Password Credentials grant];
* `authorization_code` : enables the [Authorization Code grant];
* `device_code` : enables the [Device Authorization grant];
* `token_exchange` : enables the [Token Exchange grant].

[Client Credentials grant]: https://datatracker.ietf.org/doc/html/rfc6749#section-4.4
[Resource Owner Password Credentials grant]: https://datatracker.ietf.org/doc/html/rfc6749#section-4.3
[Authorization Code grant]: https://datatracker.ietf.org/doc/html/rfc6749#section-4.1
[Device Authorization grant]: https://datatracker.ietf.org/doc/html/rfc8628
[Token Exchange grant]: https://datatracker.ietf.org/doc/html/rfc8693

!!! note
    The Device Authorization grant can also be specified using its canonical URN: 
    `urn:ietf:params:oauth:grant-type:device_code`.

!!! note
    The Token Exchange grant can also be specified using its canonical URN: 
    `urn:ietf:params:oauth:grant-type:token-exchange`.

The `client_credentials` grant type is the simplest one, but it requires the client to be granted
enough permissions to access the Nessie server on behalf of the user. This is not always possible,
and should be avoided if the session is interactive (that is, when the client is being controlled
by a human).

For this grant type, the following properties must be provided:

* `nessie.authentication.oauth2.issuer-url` or `nessie.authentication.oauth2.token-endpoint`;
* `nessie.authentication.oauth2.client-id`;
* `nessie.authentication.oauth2.client-secret` (unless the client is public).

The `password` grant type is also simple, but it requires passing the user's password to the client,
which may not be acceptable in some cases for security reasons. Many identity providers forbid its
usage. 

All the properties required for `client_credentials` are also required for this grant type, as well
as the following ones:

* `nessie.authentication.oauth2.username`;
* `nessie.authentication.oauth2.password`.

For real users trying to authenticate within a terminal session, such as a Spark shell, the
`authorization_code` grant type is recommended. It requires the user to authenticate in a browser
window, thus sparing the need to provide the user's password directly to the client. The user will
be prompted to authenticate in a separate browser window, and the Nessie client will be notified
when the authentication is complete. 

All the properties required for `client_credentials` are also required for this grant type. As
explained above, if `nessie.authentication.oauth2.issuer-url` is provided, then no further
configuration is required. Otherwise, in addition to the token endpoint, the authorization endpoint
must also be provided (`nessie.authentication.oauth2.auth-endpoint`).

If the terminal session is running remotely however, on inside an embedded device, then the
`authorization_code` grant type may not be suitable, as the browser and the terminal session must be
running on the same machine. In this case, the `device_code` grant type is recommended. Similar to
the `authorization_code` grant type, it requires the user to authenticate in a browser window, but
it does not require the browser and the terminal session to be running on the same machine. The user
will be prompted to authenticate in a local browser window, and the remote Nessie client will poll
the OAuth2 server for the authentication status, until the authentication is complete. 

All the properties required for `client_credentials` are also required for this grant type. As
explained above, if `nessie.authentication.oauth2.issuer-url` is provided, then no further
configuration is required. Otherwise, in addition to the token endpoint, the device authorization
endpoint must also be provided (`nessie.authentication.oauth2.device-auth-endpoint`).

Finally, the `token_exchange` grant type is the most complex one. In-depth configuration of a token
exchange grant is outside the scope of this document but in general, two use cases can be envisaged:

1. Initial token exchange: enabled when `authentication.oauth2.grant-type` is `token_exchange`. In
   this scenario, the client will use token exchange as the primary grant. A subject token must be
   provided with `nessie.authentication.oauth2.token-exchange.subject-token`. Other properties under
   `nessie.authentication.oauth2.token-exchange.*` may also be required.

2. Impersonation or delegation: this is the most typical usage, enabled when
   `nessie.authentication.oauth2.impersonation.enabled` is `true`. Here, the client will first
   obtain an initial token using another grant type, then exchange the received access token for
   another access token, possibly from a second OAuth2 server. If a second OAuth2 server must be
   contacted, use the properties under `nessie.authentication.oauth2.impersonation.*`. And finally,
   since impersonation uses the token exchange grant type behind the scenes, properties under
   `nessie.authentication.oauth2.token-exchange.*` may also be relevant.

!!! warning
    When using impersonation, the property `authentication.oauth2.grant-type` _must_ be another
    grant type than `token_exchange`.

!!! warning
    If a second OAuth2 server is required to perform impersonation, the admin user is responsible 
    for configuring the trust relationship between the two servers.