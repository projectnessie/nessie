# TLS Ingress with Minikube

In this guide we walk through the process of configuring a Nessie Service with secure HTTPS transport in
[Minikube](https://minikube.sigs.k8s.io/docs/).

Please familiarize yourself with the [Nessie Helm chart](../guides/kubernetes.md) and with our
general-purpose [Minikube guide](./minikube.md) before proceeding.

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

For macOS, use the following command:

```shell
sudo security add-trusted-cert -d -r trustRoot -k /Library/Keychains/System.keychain nessie.crt
```

You will need to input your password both on the command line and in a pop-up window in order to add the certificate to
the system keychain. To check that the certificate was added successfully, run the following command:

```shell
security find-certificate -c Nessie
```

The output should start with something like below:

```
keychain: "/Library/Keychains/System.keychain"
version: 256
class: 0x80001000 
attributes:
    "alis"<blob>="Nessie"
```

Note: to remove the certificate from the system keychain when you are done with this guide, run the following command:

```shell
sudo security delete-certificate -c Nessie -t /Library/Keychains/System.keychain
```

Note: Some tools allow to input certificates on the command line (e.g. `curl --cacert nessie.crt https://$(minikube
ip)/api/v2/config" ...`) If you intend to use only those tools, then you don't need to add the certificate to the system
keychain. However, browsers and Nessie clients both rely on the OS to trust the certificate, so to be able to use Nessie
UI or CLI, the simplest approach is to add the certificate to the OS keychain as explained above.

## Creating a k8s Secret for the Nessie Certificate

Make sure a dedicated k8s namespace exists. This example uses the `nessie-ns` name. If it does not exist run the
following command to create it:

```shell
kubectl create namespace nessie-ns
```

Create a TLS secret for Nessie:

```shell
kubectl -n nessie-ns create secret tls nessie-tls \
  --cert=nessie.crt --key=nessie-key.pem
```

## Deploying Nessie with Helm

Add the Nessie repository to Helm:

```shell
helm repo add nessie-helm https://charts.projectnessie.org
```

Install the Nessie helm chart:

```shell
helm install nessie -n nessie-ns nessie-helm/nessie \
  --set 'ingress.enabled=true' \
  --set 'ingress.hosts[0].host=nessie.local' \
  --set 'ingress.hosts[0].paths[0]=/' \
  --set 'ingress.tls[0].secretName=nessie-tls' \
  --set 'ingress.tls[0].hosts[0]=nessie.local'
```

The deployment process may take some time. Use the following command to check its status and get the ingress IP address.

```shell
kubectl get ingress -n nessie-ns
```

Wait until you see something like below (note the `ADDRESS` column with a non-empty value):

```
NAME     CLASS   HOSTS          ADDRESS        PORTS     AGE
nessie   nginx   nessie.local   192.168.49.2   80, 443   33s
```

Note: in minikube, the ingress IP address is the same as the minikube IP address. You can get it by running `minikube
ip` as well.

## Activating minikube's SSH tunnel

For macOS users only, an extra step is required. Run the following command after installing the Nessie helm chart:

```shell
minikube tunnel
```

This will create an SSH tunnel to services deployed with type LoadBalancer and set their Ingress to
their ClusterIP. This is required because Docker on macOS does not expose ingress ports on the host
machine. 

You should see something like below:

```
✅  Tunnel successfully started
📌  NOTE: Please do not close this terminal as this process must stay alive for the tunnel to be accessible ...
❗  The service/ingress nessie requires privileged ports to be exposed: [80 443]
🔑  sudo permission will be asked for it.
🏃  Starting tunnel for service nessie.
Password:
```

Input your password, and do not close the terminal until you are done with the guide.

## Modifying /etc/hosts

Add an entry in the local hosts file (e.g. `/etc/hosts`) mapping the ingress IP address to `nessie.local`, for example:

```
192.168.49.2	nessie.local
```

For macOS users, or users having executed the `minikube tunnel` step: you should use 127.0.0.1 instead:

```
127.0.0.1  nessie.local
```

Note: Some tools allow substituting host names with specific IP addresses on the command line
(e.g. `curl --resolve "nessie.local:443:$(minikube ip)" ...`) If you intend to use only those tools, then modifying
`/etc/hosts` is not necessary. However, Nessie python CLI and java client rely on the OS to be able to resolve
`nessie.local`, so to be able to use them the simplest approach is to define `nessie.local` in `/etc/hosts`.

## Accessing Nessie REST API over HTTPS

It's finally time to test Nessie's REST API. Use `curl` to verify that the server is accessible:

```shell
curl https://nessie.local/api/v2/config
```

The output should be similar to below:

```json
{
  "defaultBranch" : "main",
  "minSupportedApiVersion" : 1,
  "maxSupportedApiVersion" : 2,
  "actualApiVersion" : 2,
  "specVersion" : "2.0.0",
  "noAncestorHash" : "2e1cfa82b035c26cbbbdae632cea070514eb8b773f616aaeaf668e2f0be8f10d",
  "repositoryCreationTimestamp" : "2023-06-02T16:46:01.307506877Z",
  "oldestPossibleCommitTimestamp" : "2023-06-02T16:46:01.307506877Z"
}
```

## Accessing Nessie UI over HTTPS

Point your browser to https://nessie.local.

Please note that most browsers do not trust self-signed certificates, so an exception will have to be manually
granted to the "Nessie" certificate.

Once the browser is told to trust the certificate, you should see the UI landing page under 
https://nessie.local/tree/main.
