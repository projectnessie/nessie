# Multiplatform Docker build support scripts

The `build-push-images.sh` script is meant to build Java and optionally native images. Usage is not
restricted to (or specialized for) `:nessie-quarkus`, but can be used with basically any Quarkus
application.

Minimal options (use `./build-push-images.sh --help` for an up-to-date listing) are:

* `--gradle-project <gradle-project-name>` in the `:my-project-name` syntax
* `--project-dir <gradle-project-directory>` like `servers/quarkus-server`
* the image name as an argument, for example `projectnessie/nessie`

The image name also defines where the built images are pushed to. By (Docker) default, it's Docker
Hub.

## Push to a local Docker registry (`localhost:5000`)

To push to a locally running Docker registry, prefix the image name with `localhost:5000/`. For
example (see local Docker registry notes below):

```bash
tools/dockerbuild/build-push-images.sh \
  --gradle-project :nessie-quarkus \
  --project-dir servers/quarkus-server \
  localhost:5000/projectnessie/nessie-local
```

Hint: The images are pushed to the local registry, those will **not** show up in `docker images`.

## Build an image for local use

```bash
tools/dockerbuild/build-push-images.sh \
  --gradle-project :nessie-quarkus \
  --project-dir servers/quarkus-server \
  --local \
  localhost/projectnessie/nessie-local
```

## Updating your local Docker registry

There's a `docker-registry` Debian/Ubuntu package. The registry configuration might need some tweaks
like this. You can also use
the [official Docker registry](https://docs.docker.com/registry/deploying/).

* `http.addr` may default to `::1`, which is TCP6, and the Docker tools might not be able to connect
  to it.
  Change `http.addr` to `127.0.0.1:5000`.
* Change `auth.htpasswd.path` to `/etc/docker/registry/htpasswd`
* Create the htpasswd file - for example: `htpasswd -B $(id -un)`,
  if `/etc/docker/registry/htpasswd` doesn't yet exist, add the `-c` option.
* Login to your local registry using `docker login -u $(id -un) localhost:5000`
