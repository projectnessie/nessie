# Nessie behind a Reverse Proxy

If you run Nessie behind a Reverse Proxy like istio or nginx, both the reverse proxy and Nessie/Quarkus need to be
configured accordingly.

!!! warn
    Make sure to understand what the configuration options mean, before you apply those to any production setup!

!!! info
    Passing the `X-Forwarded-Proto`, `X-Forwarded-Host` and `X-Forwarded-Port` headers to Nessie works for
    Iceberg REST.

The follow config options are mentioned only for documentation purposes. Consult the
[Quarkus documentation](https://quarkus.io/guides/http-reference#reverse-proxy) for "Running behind a reverse proxy"
and configure those depending on your actual needs.

=== "Application Properties"
    ```properties
    quarkus.http.proxy.proxy-address-forwarding=true
    quarkus.http.proxy.allow-x-forwarded=true
    quarkus.http.proxy.enable-forwarded-host=true
    quarkus.http.proxy.enable-forwarded-prefix=true
    ```

=== "Helm Chart"
    ```yaml
    advancedConfig:
      quarkus:
        http:
          proxy:
            proxy-address-forwarding: "true"
            allow-x-forwarded: "true"
            enable-forwarded-host: "true"
            enable-forwarded-prefix: "true"
    ```

!!! warn
    Do NOT enable the above options unless your reverse proxy (for example istio or nginx)
    is properly setup to set these headers but also filter those from incoming requests.

!!! info
    The newer `Forwarded` header is the newer version of the older `X-Forwarded-For`, `X-Forwarded-Host`,
    `X-Forwarded-Proto` headers, but does _not_ provide a replacement for the `X-Forwarded-Prefix` header,
    which is needed in some scenarios.

## Using path prefixes on the ingress / reverse proxy

Usually, all HTTP requests to the reverse proxy are passed down to Nessie. If only requests that
start with for example `/nessie/` to be proxied to Nessie, then the reverse proxy needs to be
configured to also pass the `X-Forwarded-Prefix` with the that prefix and Quarkus must be configured
to respect the `X-Forwarded-Prefix` header using the `quarkus.http.proxy.enable-forwarded-prefix=true`
configuration.

## Verifying the reverse proxy configuration

Nessie returns URLs via the Iceberg REST config endpoint and in the returned table/view metadata. These
URLs are used by Iceberg clients to issue follow-up requests.

Assuming the setup of the below Docker Compose example, issue the following `curl` command against your
ingress (reverse proxy).

```shell
curl https://nessie-nginx.localhost.localdomain:8443/nessie/iceberg/v1/config
```

It should yield the configuration. Look for the `nessie.iceberg-base-uri` property:
```json5
{
  "defaults": {
    // ...
  },
  "overrides": {
    // ...
    "nessie.iceberg-base-uri": "https://nessie-nginx.localhost.localdomain:8443/nessie/iceberg/"
    // ...
  }
}
```

Make sure that the scheme (`https` in this case), the hostname (`nessie-nginx.localhost.localdomain`
in this case), the port (`8443` in this case) and the prefix (`/nessie/` in this case) match the
expected values. Differences _will_ result in Iceberg REST request failures.

Consult the documentation of your ingress/reverse-proxy for details how to set those up to meet the
requirements of your particular environment.

=== "Istio / Envoy"
    Related istio/envoy documentation pages:
    
    * [Configuring X-Forwarded-For](https://istio.io/latest/docs/ops/configuration/traffic-management/network-topologies/#configuring-x-forwarded-for-headers)
    * [Header manipulation](https://www.envoyproxy.io/docs/envoy/latest/configuration/http/http_conn_man/headers)

=== "Nginx"
    Related nginx documentation pages:
    
    * [Reverse Proxy](https://docs.nginx.com/nginx/admin-guide/web-server/reverse-proxy/#passing-a-request-to-a-proxied-server)
    * [Proxy module](http://nginx.org/en/docs/http/ngx_http_proxy_module.html)
    * [Load Balancer](https://nginx.org/en/docs/http/load_balancing.html)
    
    See the [example configuration](https://github.com/projectnessie/nessie/tree/main/docker/catalog-nginx-https/nginx.conf) from the [Docker Compose example](#docker-compose-example) below:
    ```apacheconf
    events {
      worker_connections 1024;
    }
    
    http {
      # Redirect non-HTTPS to HTTPS
      server {
        listen 8080;
        server_name nessie-nginx.localhost.localdomain;
        return 301 https://$host$request_uri;
      }
    
      server {
        listen 8443 ssl;
        server_name nessie-nginx.localhost.localdomain;
    
        ssl_certificate /etc/nginx/certs/nessie-nginx.localhost.localdomain+3.pem;
        ssl_certificate_key /etc/nginx/certs/nessie-nginx.localhost.localdomain+3-key.pem;
        ssl_protocols TLSv1 TLSv1.1 TLSv1.2;
        ssl_ciphers HIGH:!aNULL:!MD5;
    
        # This example uses /nessie/ as the path-prefix. It is not mandatory to do this.
        # To use no prefix and route all requests to Nessie, set '/' as the 'location' and
        # remove the 'proxy_set_header X-Forwarded-Prefix' line.
        location /nessie/ {
          proxy_buffering off;
          # The X-Forwarded-* headers needed by Quarkus
          proxy_set_header X-Forwarded-Proto $scheme;
          proxy_set_header X-Forwarded-Host $host;
          proxy_set_header X-Forwarded-Port $server_port;
          # X-Forwarded-Prefix is needed when the ingress shall use prefixes. Must set
          # quarkus.http.proxy.enable-forwarded-prefix=true for Nessie/Quarkus in that case.
          proxy_set_header X-Forwarded-Prefix /nessie/;
    
          proxy_pass http://nessie:19120;
        }
      }
    }
    ```

## Docker Compose example

!!! info
    Either docker-compose or podman-compose are needed.

We have a [Docker Compose example](https://github.com/projectnessie/nessie/tree/main/docker/catalog-nginx-https) to
illustrate how to use Nessie behind a reverse proxy, using nginx in this example. You need a local clone of the
[Nessie source repository](https://github.com/projectnessie/nessie) to run this example.

Create a self-signed certificate first, we use the `mkcert` tool.
```shell
# Install the mkcert tool
sudo apt install mkcert
# Run the setup script - it will generate the necessary SSL certificate.
docker/catalog-nginx-https/setup.sh
```

Build Nessie Docker image
```shell
tools/dockerbuild/build-push-images.sh --gradle-project :nessie-quarkus  --project-dir servers/quarkus-server --local localhost/projectnessie/nessie
```

Then start the Docker compose:

=== "Docker"
    ```shell
    docker-compose -f docker/catalog-nginx-https/docker-compose.yml up
    ```
=== "Podman"
    ```shell
    podman-compose -f docker/catalog-nginx-https/docker-compose.yml up
    ```
 
Run `spark-sql` against Nessie running behind the reverse proxy:
```shell
catalog/bin/spark-sql.sh --no-nessie-start --aws --iceberg https://nessie-nginx.localhost.localdomain:8443/nessie/iceberg/main
```

Within the Spark SQL shell:
```sql
CREATE NAMESPACE nessie.sales;
USE nessie.sales;
CREATE TABLE city (C_CITYKEY BIGINT, C_NAME STRING, N_NATIONKEY BIGINT, C_COMMENT STRING) USING iceberg PARTITIONED BY (bucket(16, N_NATIONKEY));
```

Exit the Spark SQL shell.

Run the Nessie CLI:
```shell
./gradlew :nessie-cli:clean :nessie-cli:jar

java -jar cli/cli/build/libs/nessie-cli-*-SNAPSHOT.jar
```

In Nessie CLI:
```sql
CONNECT TO https://nessie-nginx.localhost.localdomain:8443/nessie/api/v2
```
It will print the following informational messages:
```
Connecting to https://nessie-nginx.localhost.localdomain:8443/nessie/api/v2 ...
Successfully connected to Iceberg REST at https://nessie-nginx.localhost.localdomain:8443/nessie/iceberg/
Successfully connected to Nessie REST at https://nessie-nginx.localhost.localdomain:8443/nessie/api/v2/ - Nessie API version 2, spec version 2.1.0
```

More CLI commands to try out:
```sql
LIST CONTENTS
SHOW TABLE sales.city
```
