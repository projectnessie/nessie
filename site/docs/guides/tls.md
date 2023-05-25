# TLS Ingress with Minikube

In this guide we walk through the process of configuring a Nessie Service with secure HTTPS transport in
[Minikube](https://minikube.sigs.k8s.io/docs/).

## Setting up Minikube

1. Start Minikube cluster: `minikube start`
2. Enable NGINX Ingress controller: `minikube addons enable ingress`

## Creating Certificates

If you have your own trusted certificate you can use it to sign a new certificate for the Nessie service.

Otherwise, the rest of this section shows how can create a self-signed certificate and add it the list of trusted
certificates in the client OS.

To generate a fresh pair of a key and a (self-signed) certificate:

```shell
openssl req -new -subj "/CN=Nessie" -addext "subjectAltName = DNS:nessie.local" \
  -newkey rsa:2048 -keyout nessie-key.pem -out nessie.crt -x509 -nodes
```

Note the `-nodes` option. It is used only for the sake of simplicity of this example deployment. Also, newer `openssl`
versions deprecated it in favour of `-noenc`.

Add the new certificate to the local set of CA certificates on the client host (where the Nessie clients,
such as Nessie CLI / `curl` / browser, are going to run).

The following example is for Linux:

1. `sudo cp nessie.crt /usr/local/share/ca-certificates/nessie.crt`
2. `sudo update-ca-certificates`

This should output something like "Certificate added: CN=Nessie".

## Creating a k8s Secret for the Nessie Certificate

```shell
kubectl -n nessie-ns create secret tls nessie-tls \
  --cert=nessie.crt --key=nessie-key.pem
```

## Deploying Nessie with Helm

```shell
helm install nessie -n nessie-ns helm/nessie \
  --set ingress.enabled=true \
  --set ingress.hosts[0].host='nessie.local' \
  --set ingress.hosts[0].paths[0]='/' \
  --set ingress.tls[0].secretName=nessie-tls \
  --set ingress.tls[0].hosts[0]='nessie.local'
```

The deployment process may take some time. Use the following command to check its status and get the ingress IP address.

```shell
kubectl get ingress -n nessie-ns
```

Add an entry in the local hosts file (e.g. `/etc/hosts`) mapping that IP address to `nessie.local`, for example:

```shell
192.168.49.2	nessie.local
```

Use `curl` to verify that the server is accessible:

```shell
$ curl https://nessie.local/api/v2/config
{
  "defaultBranch" : "main",
  "minSupportedApiVersion" : 1,
  "maxSupportedApiVersion" : 2,
  "actualApiVersion" : 2,
  "specVersion" : "2.0.0"
}
```

## Accessing Nessie UI over HTTPS

Please note that most browsers do not trust self-signed certificates, so an exception will have to be manually
granted to the "Nessie" certificate.
