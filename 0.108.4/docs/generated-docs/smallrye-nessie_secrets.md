Secrets manager and mapping configuration. 

Currently the following secrets managers are supported:   

Secrets can always be provided using [Quarkus' built-in  mechanisms ](#providing-secrets). Additionally, the following external secrets managers can be enabled:   

 * `VAULT` Hashicorp Vault. See the [Quarkus        docs for Hashicorp Vault ](https://docs.quarkiverse.io/quarkus-vault/dev/index.html#configuration-reference) for specific information.    
 * `AMAZON` AWS Secrets Manager. See the [Quarkus        docs for Amazon Services / Secrets Manager ](https://docs.quarkiverse.io/quarkus-amazon-services/dev/amazon-secretsmanager.html#_configuration_reference) for specific information.    
 * `AZURE` AWS Secrets Manager. **NOT SUPPORTED YET!** See the [Quarkus        docs for Azure Key Vault ](https://docs.quarkiverse.io/quarkus-azure-services/dev/quarkus-azure-key-vault.html#_extension_configuration_reference) for specific information.    
 * `GOOGLE` Google Cloud Secrets Manager. **NOT SUPPORTED YET!** 

For details how secrets are stored, see [below](#types-of-secrets)

| Property | Default Value | Type | Description |
|----------|---------------|------|-------------|
| `nessie.secrets.type` |  | `ExternalSecretsManagerType` | Choose the secrets manager to use, defaults to no secrets manager.  |
| `nessie.secrets.path` |  | `string` | The path/prefix used when accessing secrets from the secrets manager. <br><br>This setting can be useful, if all Nessie related secrets have the same prefix in your  external secrets manager.  |
| `nessie.secrets.cache.enabled` | `true` | `boolean` | Flag whether the secrets cache is enabled.  |
| `nessie.secrets.cache.max-elements` | `1000` | `long` | Maximum number of cached secrets.  |
| `nessie.secrets.cache.ttl` | `PT15M` | `duration` | Time until cached secrets expire.  |
| `nessie.secrets.get-secret-timeout` | `PT2S` | `duration` | Timeout when retrieving a secret from the external secret manager, not supported for AWS.  |
