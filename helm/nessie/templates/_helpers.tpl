{{/**

  Copyright (C) 2024 Dremio

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.

**/}}

{{/*
Expand the name of the chart.
*/}}
{{- define "nessie.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "nessie.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "nessie.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "nessie.labels" -}}
helm.sh/chart: {{ include "nessie.chart" . }}
{{ include "nessie.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.Version | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "nessie.selectorLabels" -}}
app.kubernetes.io/name: {{ include "nessie.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "nessie.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "nessie.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Convert a dict into a string formed by a comma-separated list of key-value pairs: key1=value1,key2=value2, ...
*/}}
{{- define "nessie.dictToString" -}}
{{- $list := list -}}
{{- range $k, $v := . -}}
{{- $list = append $list (printf "%s=%s" $k $v) -}}
{{- end -}}
{{ join "," $list }}
{{- end -}}

{{- define "nessie.mergeAdvancedConfig" -}}
{{- $advConfig := index . 0 -}}
{{- $prefix := index . 1 -}}
{{- $dest := index . 2 -}}
{{- range $key, $val := $advConfig -}}
{{- $name := ternary $key (print $prefix "." $key) (eq $prefix "") -}}
{{- if kindOf $val | eq "map" -}}
{{- list $val $name $dest | include "nessie.mergeAdvancedConfig" -}}
{{- else -}}
{{- $_ := set $dest $name $val -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Determine the datasource kind based on the jdbcUrl. This relies on the fact that datasource
names should coincide with jdbc schemes in connection URIs.
*/}}
{{- define "nessie.dbKind" -}}
{{- $v := . | split ":" -}}
{{ $v._1 }}
{{- end }}

{{/*
Apply Nessie Catalog (Iceberg REST) options.
*/}}
{{- define "nessie.applyCatalogIcebergOptions" -}}
{{- $root := index . 0 -}}{{/* the object to introspect */}}
{{- $map := index . 1 -}}{{/* the destination map */}}
{{- with $root -}}
{{- $_ := set $map "nessie.catalog.default-warehouse" .defaultWarehouse -}}
{{- $_ = set $map "nessie.catalog.object-stores.health-check.enabled" .objectStoresHealthCheckEnabled -}}
{{- range $k, $v := .configDefaults -}}
{{- $_ = set $map ( printf "nessie.catalog.iceberg-config-defaults.%s" $k ) $v -}}
{{- end -}}
{{- range $k, $v := .configOverrides -}}
{{- $_ = set $map ( printf "nessie.catalog.iceberg-config-overrides.%s" $k ) $v -}}
{{- end -}}
{{- range $i, $warehouse := .warehouses -}}
{{- if not $warehouse.name -}}{{- required ( printf "catalog.iceberg.warehouses[%d]: missing warehouse name" $i ) $warehouse.name -}}{{- end -}}
{{- $_ = set $map ( printf "nessie.catalog.warehouses.%s.location" ( quote $warehouse.name ) ) $warehouse.location -}}
{{- range $k, $v := $warehouse.configDefaults -}}
{{- $_ = set $map ( printf "nessie.catalog.warehouses.%s.iceberg-config-defaults.%s" ( quote $warehouse.name ) $k ) $v -}}
{{- end -}}
{{- range $k, $v := $warehouse.configOverrides -}}
{{- $_ = set $map ( printf "nessie.catalog.warehouses.%s.iceberg-config-overrides.%s" ( quote $warehouse.name ) $k ) $v -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Apply S3 catalog options.
*/}}
{{- define "nessie.applyCatalogStorageS3RootOptions" -}}
{{- $root := index . 0 -}}{{/* the object to introspect */}}
{{- $prefix := index . 1 -}}{{/* the current prefix */}}
{{- $map := index . 2 -}}{{/* the destination map */}}
{{- with $root -}}
{{- if .transport -}}
{{- include "nessie.addConfigOption" (list .transport.maxHttpConnections $map ( print $prefix "http.max-http-connections" )) -}}
{{- include "nessie.addConfigOption" (list .transport.readTimeout $map ( print $prefix "http.read-timeout" )) -}}
{{- include "nessie.addConfigOption" (list .transport.connectTimeout $map ( print $prefix "http.connect-timeout" )) -}}
{{- include "nessie.addConfigOption" (list .transport.connectionAcquisitionTimeout $map ( print $prefix "http.connection-acquisition-timeout" )) -}}
{{- include "nessie.addConfigOption" (list .transport.connectionMaxIdleTime $map ( print $prefix "http.connection-max-idle-time" )) -}}
{{- include "nessie.addConfigOption" (list .transport.connectionTimeToLive $map ( print $prefix "http.connection-time-to-live" )) -}}
{{- include "nessie.addConfigOption" (list .transport.expectContinueEnabled $map ( print $prefix "http.expect-continue-enabled" )) -}}
{{- end -}}
{{- if .sessionCredentials }}
{{- include "nessie.addConfigOption" (list .sessionCredentials.sessionCredentialRefreshGracePeriod $map ( print $prefix "sts.session-grace-period" )) -}}
{{- include "nessie.addConfigOption" (list .sessionCredentials.sessionCredentialCacheMaxEntries $map ( print $prefix "sts.session-cache-max-size" )) -}}
{{- include "nessie.addConfigOption" (list .sessionCredentials.stsClientsCacheMaxEntries $map ( print $prefix "sts.clients-cache-max-size" )) -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "nessie.applyCatalogStorageS3BucketOptions" -}}
{{- $root := index . 0 -}}{{/* the object to introspect */}}
{{- $prefix := index . 1 -}}{{/* the current prefix */}}
{{- $map := index . 2 -}}{{/* the destination map */}}
{{- with $root -}}
{{- include "nessie.addConfigOption" (list .name $map ( print $prefix "name" )) -}}
{{- include "nessie.addConfigOption" (list .authority $map ( print $prefix "authority" )) -}}
{{- include "nessie.addConfigOption" (list .pathPrefix $map ( print $prefix "path-prefix" )) -}}
{{- include "nessie.addConfigOption" (list .region $map ( print $prefix "region" )) -}}
{{- include "nessie.addConfigOption" (list .endpoint $map ( print $prefix "endpoint" )) -}}
{{- include "nessie.addConfigOption" (list .externalEndpoint $map ( print $prefix "external-endpoint" )) -}}
{{- include "nessie.addConfigOption" (list .pathStyleAccess $map ( print $prefix "path-style-access" )) -}}
{{- include "nessie.addConfigOption" (list .accessPoint $map ( print $prefix "access-point" )) -}}
{{- include "nessie.addConfigOption" (list .allowCrossRegionAccessPoint $map ( print $prefix "allow-cross-region-access-point" )) -}}
{{- include "nessie.addConfigOption" (list .requestSigningEnabled $map ( print $prefix "request-signing-enabled" )) -}}
{{- include "nessie.addConfigOption" (list .authType $map ( print $prefix "auth-type" )) -}}
{{- include "nessie.addConfigOption" (list .stsEndpoint $map ( print $prefix "sts-endpoint" )) -}}
{{- if .clientIam -}}
{{- include "nessie.addConfigOption" (list .clientIam.enabled $map ( print $prefix "client-iam.enabled" )) -}}
{{- include "nessie.addConfigOption" (list .clientIam.policy $map ( print $prefix "client-iam.policy" )) -}}
{{- include "nessie.addConfigOption" (list .clientIam.roleArn $map ( print $prefix "client-iam.assume-role" )) -}}
{{- include "nessie.addConfigOption" (list .clientIam.roleSessionName $map ( print $prefix "client-iam.role-session-name" )) -}}
{{- include "nessie.addConfigOption" (list .clientIam.externalId $map ( print $prefix "client-iam.external-id" )) -}}
{{- include "nessie.addConfigOption" (list .clientIam.sessionDuration $map ( print $prefix "client-iam.session-duration" )) -}}
{{- if .clientIam.statements -}}
{{- range $i, $statement := .clientIam.statements -}}
{{- $_ := set $map ( printf "%sclient-iam.statements[%d]" $prefix $i ) $statement -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- if .serverIam -}}
{{- include "nessie.addConfigOption" (list .serverIam.enabled $map ( print $prefix "server-iam.enabled" )) -}}
{{- include "nessie.addConfigOption" (list .serverIam.policy $map ( print $prefix "server-iam.policy" )) -}}
{{- include "nessie.addConfigOption" (list .serverIam.roleArn $map ( print $prefix "server-iam.ssume-role" )) -}}
{{- include "nessie.addConfigOption" (list .serverIam.roleSessionName $map ( print $prefix "server-iam.role-session-name" )) -}}
{{- include "nessie.addConfigOption" (list .serverIam.externalId $map ( print $prefix "server-iam.external-id" )) -}}
{{- include "nessie.addConfigOption" (list .serverIam.sessionDuration $map ( print $prefix "server-iam.session-duration" )) -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Apply GCS catalog options.
*/}}
{{- define "nessie.applyCatalogStorageGcsRootOptions" -}}
{{- $root := index . 0 -}}{{/* the object to introspect */}}
{{- $prefix := index . 1 -}}{{/* the current prefix */}}
{{- $map := index . 2 -}}{{/* the destination map */}}
{{- with $root -}}
{{- if .transport -}}
{{- include "nessie.addConfigOption" (list .transport.maxAttempts $map ( print $prefix "max-attempts" )) -}}
{{- include "nessie.addConfigOption" (list .transport.connectTimeout $map ( print $prefix "connect-timeout" )) -}}
{{- include "nessie.addConfigOption" (list .transport.readTimeout $map ( print $prefix "read-timeout" )) -}}
{{- include "nessie.addConfigOption" (list .transport.initialRetryDelay $map ( print $prefix "initial-retry-delay" )) -}}
{{- include "nessie.addConfigOption" (list .transport.maxRetryDelay $map ( print $prefix "max-retry-delay" )) -}}
{{- include "nessie.addConfigOption" (list .transport.retryDelayMultiplier $map ( print $prefix "retry-delay-multiplier" )) -}}
{{- include "nessie.addConfigOption" (list .transport.initialRpcTimeout $map ( print $prefix "initial-rpc-timeout" )) -}}
{{- include "nessie.addConfigOption" (list .transport.maxRpcTimeout $map ( print $prefix "max-rpc-timeout" )) -}}
{{- include "nessie.addConfigOption" (list .transport.rpcTimeoutMultiplier $map ( print $prefix "rpc-timeout-multiplier" )) -}}
{{- include "nessie.addConfigOption" (list .transport.logicalTimeout $map ( print $prefix "logical-timeout" )) -}}
{{- include "nessie.addConfigOption" (list .transport.totalTimeout $map ( print $prefix "total-timeout" )) -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "nessie.applyCatalogStorageGcsBucketOptions" -}}
{{- $root := index . 0 -}}{{/* the object to introspect */}}
{{- $prefix := index . 1 -}}{{/* the current prefix */}}
{{- $map := index . 2 -}}{{/* the destination map */}}
{{- with $root -}}
{{- include "nessie.addConfigOption" (list .name $map ( print $prefix "name" )) -}}
{{- include "nessie.addConfigOption" (list .authority $map ( print $prefix "authority" )) -}}
{{- include "nessie.addConfigOption" (list .pathPrefix $map ( print $prefix "path-prefix" )) -}}
{{- include "nessie.addConfigOption" (list .host $map ( print $prefix "host" )) -}}
{{- include "nessie.addConfigOption" (list .externalHost $map ( print $prefix "external-host" )) -}}
{{- include "nessie.addConfigOption" (list .userProject $map ( print $prefix "user-project" )) -}}
{{- include "nessie.addConfigOption" (list .projectId $map ( print $prefix "project-id" )) -}}
{{- include "nessie.addConfigOption" (list .quotaProjectId $map ( print $prefix "quota-project-id" )) -}}
{{- include "nessie.addConfigOption" (list .clientLibToken $map ( print $prefix "client-lib-token" )) -}}
{{- include "nessie.addConfigOption" (list .authType $map ( print $prefix "auth-type" )) -}}
{{- include "nessie.addConfigOption" (list .encryptionKey $map ( print $prefix "encryption-key" )) -}}
{{- include "nessie.addConfigOption" (list .decryptionKey $map ( print $prefix "decryption-key" )) -}}
{{- include "nessie.addConfigOption" (list .readChunkSize $map ( print $prefix "read-chunk-size" )) -}}
{{- include "nessie.addConfigOption" (list .writeChunkSize $map ( print $prefix "write-chunk-size" )) -}}
{{- include "nessie.addConfigOption" (list .deleteBatchSize $map ( print $prefix "delete-batch-size" )) -}}
{{- end -}}
{{- end -}}

{{/*
Apply ADLS catalog options.
*/}}
{{- define "nessie.applyCatalogStorageAdlsRootOptions" -}}
{{- $root := index . 0 -}}{{/* the object to introspect */}}
{{- $prefix := index . 1 -}}{{/* the current prefix */}}
{{- $map := index . 2 -}}{{/* the destination map */}}
{{- with $root -}}
{{- if .transport -}}
{{- include "nessie.addConfigOption" (list .transport.maxHttpConnections $map ( print $prefix "max-http-connections" )) -}}
{{- include "nessie.addConfigOption" (list .transport.connectTimeout $map ( print $prefix "connect-timeout" )) -}}
{{- include "nessie.addConfigOption" (list .transport.readTimeout $map ( print $prefix "read-timeout" )) -}}
{{- include "nessie.addConfigOption" (list .transport.writeTimeout $map ( print $prefix "write-timeout" )) -}}
{{- include "nessie.addConfigOption" (list .transport.connectionIdleTimeout $map ( print $prefix "connection-idle-timeout" )) -}}
{{- include "nessie.addConfigOption" (list .transport.readBlockSize $map ( print $prefix "read-block-size" )) -}}
{{- include "nessie.addConfigOption" (list .transport.writeBlockSize $map ( print $prefix "write-block-size" )) -}}
{{- end -}}
{{- list .advancedConfig ( print $prefix "configuration" ) $map | include "nessie.mergeAdvancedConfig" }}
{{- end -}}
{{- end -}}

{{- define "nessie.applyCatalogStorageAdlsFileSystemOptions" -}}
{{- $root := index . 0 -}}{{/* the object to introspect */}}
{{- $prefix := index . 1 -}}{{/* the current prefix */}}
{{- $map := index . 2 -}}{{/* the destination map */}}
{{- with $root -}}
{{- include "nessie.addConfigOption" (list .name $map ( print $prefix "name" )) -}}
{{- include "nessie.addConfigOption" (list .authority $map ( print $prefix "authority" )) -}}
{{- include "nessie.addConfigOption" (list .pathPrefix $map ( print $prefix "path-prefix" )) -}}
{{- include "nessie.addConfigOption" (list .endpoint $map ( print $prefix "endpoint" )) -}}
{{- include "nessie.addConfigOption" (list .externalEndpoint $map ( print $prefix "external-endpoint" )) -}}
{{- include "nessie.addConfigOption" (list .retryPolicy $map ( print $prefix "retry-policy" )) -}}
{{- include "nessie.addConfigOption" (list .maxRetries $map ( print $prefix "max-retries" )) -}}
{{- include "nessie.addConfigOption" (list .tryTimeout $map ( print $prefix "try-timeout" )) -}}
{{- include "nessie.addConfigOption" (list .retryDelay $map ( print $prefix "retry-delay" )) -}}
{{- include "nessie.addConfigOption" (list .maxRetryDelay $map ( print $prefix "max-retry-delay" )) -}}
{{- include "nessie.addConfigOption" (list .authType $map ( print $prefix "auth-type" )) -}}
{{- end -}}
{{- end -}}

{{/*
Define environkent variables for catalog storage options.
*/}}
{{- define "nessie.catalogStorageEnv" -}}
{{ $global := .}}
{{- include "nessie.catalogSecretToEnv" (list .Values.catalog.storage.s3.defaultOptions.accessKeySecret "awsAccessKeyId" "s3.default-options.access-key" "name" true . ) }}
{{- include "nessie.catalogSecretToEnv" (list .Values.catalog.storage.s3.defaultOptions.accessKeySecret "awsSecretAccessKey" "s3.default-options.access-key" "secret" false . ) }}
{{- range $i, $bucket := .Values.catalog.storage.s3.buckets -}}
{{- with $global }}
{{- include "nessie.catalogSecretToEnv" (list $bucket.accessKeySecret "awsAccessKeyId" (printf "s3.buckets.bucket%d.access-key" (add $i 1)) "name" true . ) }}
{{- include "nessie.catalogSecretToEnv" (list $bucket.accessKeySecret "awsSecretAccessKey" (printf "s3.buckets.bucket%d.access-key" (add $i 1)) "secret" false . ) }}
{{- end -}}
{{- end -}}
{{- include "nessie.catalogSecretToEnv" (list .Values.catalog.storage.gcs.defaultOptions.authCredentialsJsonSecret "key" "gcs.default-options.auth-credentials-json" "key" true . ) }}
{{- include "nessie.catalogSecretToEnv" (list .Values.catalog.storage.gcs.defaultOptions.oauth2TokenSecret "token" "gcs.default-options.oauth-token" "token" true . ) }}
{{- include "nessie.catalogSecretToEnv" (list .Values.catalog.storage.gcs.defaultOptions.oauth2TokenSecret "expiresAt" "gcs.default-options.oauth-token" "expiresAt" false . ) }}
{{- range $i, $bucket := .Values.catalog.storage.gcs.buckets -}}
{{- with $global }}
{{- include "nessie.catalogSecretToEnv" (list $bucket.authCredentialsJsonSecret "key" (printf "gcs.buckets.bucket%d.auth-credentials-json" (add $i 1)) "key" true . ) }}
{{- include "nessie.catalogSecretToEnv" (list $bucket.oauth2TokenSecret "token" (printf "gcs.buckets.bucket%d.oauth-token" (add $i 1)) "token" true . ) }}
{{- include "nessie.catalogSecretToEnv" (list $bucket.oauth2TokenSecret "expiresAt" (printf "gcs.buckets.bucket%d.oauth-token" (add $i 1)) "expiresAt" false . ) }}
{{- end -}}
{{- end -}}
{{ include "nessie.catalogSecretToEnv" (list .Values.catalog.storage.adls.defaultOptions.accountSecret "accountName" "adls.default-options.account" "name" true . ) }}
{{- include "nessie.catalogSecretToEnv" (list .Values.catalog.storage.adls.defaultOptions.accountSecret "accountKey" "adls.default-options.account" "secret" false . ) }}
{{- include "nessie.catalogSecretToEnv" (list .Values.catalog.storage.adls.defaultOptions.sasTokenSecret "sasToken" "adls.default-options.sas-token" "key" true . ) }}
{{- range $i, $filesystem := .Values.catalog.storage.adls.filesystems -}}
{{- with $global }}
{{- include "nessie.catalogSecretToEnv" (list $filesystem.accountSecret "accountName" (printf "adls.file-systems.filesystem%d.account" (add $i 1)) "name" true . ) }}
{{- include "nessie.catalogSecretToEnv" (list $filesystem.accountSecret "accountKey" (printf "adls.file-systems.filesystem%d.account" (add $i 1)) "secret" false . ) }}
{{- include "nessie.catalogSecretToEnv" (list $filesystem.sasTokenSecret "sasToken" (printf "adls.file-systems.filesystem%d.sas-token" (add $i 1)) "key" true . ) }}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Define an env var from secret key.

Secrets are (can be) composite values - think of a username+password.
Secrets are not (no longer) present (or directly resolvable) from the bucket option types, but have to be resolved
via a symbolic name, which is something like 'nessie-catalog-secrets.s3.default-options.access-key'. The bucket
config types know about that symbolic name and resolve it via a SecretsProvider, which resolves via Quarkus' config.

*/}}
{{- define "nessie.catalogSecretToEnv" -}}
{{- $secret := index . 0 -}}
{{- $key := index . 1 -}}
{{- $midfix := index . 2 -}}
{{- $suffix := index . 3 -}}
{{- $addRef := index . 4 -}}
{{- $global := index . 5 -}}
{{- if $secret -}}
{{- $secretName := get $secret "name" -}}
{{- $secretKey := get $secret $key -}}
{{- with $global -}}
{{- if (and $secretName $secretKey) -}}
{{ if $addRef -}}
#
# {{ $midfix }}
#
- name: {{ (printf "nessie.catalog.service.%s" $midfix) | quote }}
  value: {{ (printf "urn:nessie-secret:quarkus:nessie-catalog-secrets.%s" $midfix) | quote }}
{{- end }}
- name: {{ (printf "nessie-catalog-secrets.%s.%s" $midfix $suffix) | quote }}
  valueFrom:
    secretKeyRef:
      name: {{ (tpl $secretName . ) | quote }}
      key: {{ (tpl $secretKey . ) | quote }}
{{ end -}}
{{- end -}}
{{- end -}}
{{- end -}}


{{/*
Define an env var from secret key.
*/}}
{{- define "nessie.secretToEnv" -}}
{{- $secret := index . 0 -}}
{{- $key := index . 1 -}}
{{- $envVarName := index . 2 -}}
{{- $global := index . 3 -}}
{{- if $secret -}}
{{- $secretName := get $secret "name" -}}
{{- $secretKey := get $secret $key -}}
{{- with $global -}}
{{- if (and $secretName $secretKey) -}}
- name: {{ $envVarName | quote }}
  valueFrom:
    secretKeyRef:
      name: {{ (tpl $secretName . ) | quote }}
      key: {{ (tpl $secretKey . ) | quote }}
{{ end -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Adds a configuration option to the map if the value is not nil. Zero-values like false or 0 are
considered valid and thus added. This template should not be applied to non-scalar values like
slices or maps.
*/}}
{{- define "nessie.addConfigOption" -}}
{{- $value := index . 0 -}}{{/* the value to add */}}
{{- $map := index . 1 -}}{{/* the destination map */}}
{{- $key := index . 2 -}}{{/* the destination map key */}}
{{- if (ne $value nil) -}}
{{- $_ := set $map $key $value -}}
{{- end -}}
{{- end -}}

{{/*
Prints the configuration option to the destination configmap entry. See confimap.yaml.
Any nil values will be printed as empty config options; otherwise, the value will be evaluated
as a template against the global context, then printed. Furthermore, if the value contains
line breaks, they will be escaped and a multi-line option will be printed.
*/}}
{{- define "nessie.appendConfigOption" -}}
{{- $key := index . 0 -}}
{{- $value := index . 1 -}}
{{- $global := index . 2 -}}
{{- $valAsString := "" -}}
{{- if ne $value nil -}}
{{- $valAsString = tpl (toString $value) $global -}}
{{- if contains "\r\n" $valAsString -}}
{{- $valAsString = $valAsString | nindent 4 | replace "\r\n" "\\\r\n" -}}
{{- else if contains "\n" $valAsString -}}
{{- $valAsString = $valAsString | nindent 4 | replace "\n" "\\\n" -}}
{{- end -}}
{{- end -}}
{{ print $key "=" $valAsString }}
{{- end -}}

{{/*
Prints the ports section of the container spec. Also validates all port names and numbers to ensure
that they are consistent and that there are no overlaps.
*/}}
{{- define "nessie.containerPorts" -}}
{{- $ports := dict -}}
{{- range $i, $port := .Values.service.ports -}}
{{- if hasKey $ports $port.name -}}
{{- fail (printf "service.ports[%d]: port name already taken: %v" $i $port.name) -}}
{{- end -}}
{{- if has $port.number (values $ports) -}}
{{- fail (printf "service.ports[%d]: port number already taken: %v" $i $port.number) -}}
{{- end -}}
{{- $_ := set $ports $port.name $port.number -}}
{{- end -}}
{{- if hasKey $ports .Values.managementService.portName -}}
{{- fail (print "managementService.portName: port name already taken: " .Values.managementService.portName ) -}}
{{- end -}}
{{- if has .Values.managementService.portNumber (values $ports) -}}
{{- fail (print "managementService.portNumber: port number already taken: " .Values.managementService.portNumber) -}}
{{- end -}}
{{- $_ := set $ports .Values.managementService.portName .Values.managementService.portNumber -}}
{{- range $i, $svc := .Values.extraServices -}}
{{- range $j, $port := $svc.ports -}}
{{- if hasKey $ports $port.name -}}
{{- if ne $port.number (get $ports $port.name) -}}
{{- fail (printf "extraServices[%d].ports[%d]: wrong port number for port %s, expected %v, got %v" $i $j $port.name (get $ports $port.name) $port.number) -}}
{{- end -}}
{{- else if has $port.number (values $ports) -}}
{{- fail (printf "extraServices[%d].ports[%d]: port number already taken: %v" $i $j $port.number) -}}
{{- end -}}
{{- $_ := set $ports $port.name $port.number -}}
{{- end -}}
{{- end -}}
ports:
{{ range $portName, $portNumber := $ports -}}
- name: {{ $portName }}
  containerPort: {{ $portNumber }}
  protocol: TCP
{{ end -}}
{{ end -}}

{{/*
Shared - Converts a Kubernetes quantity to a number (int64 if possible or float64 otherwise).
It handles raw numbers as well as quantities with suffixes
like m, k, M, G, T, P, E, ki, Mi, Gi, Ti, Pi, Ei.
It also handles scientific notation.
Quantities should be positive, so negative values, zero, or any unparseable number
will result in a failure.
https://kubernetes.io/docs/reference/kubernetes-api/common-definitions/quantity/
*/}}
{{- define "nessie.quantity" -}}
{{- $quantity := . -}}
{{- $n := $quantity | float64 -}}
{{- if kindIs "string" $quantity -}}
{{- if hasSuffix "m" $quantity -}}
{{- $n = divf (trimSuffix "m" $quantity | float64) 1000.0 -}}
{{- else if hasSuffix "k" $quantity -}}
{{- $n = trimSuffix "k" $quantity | int64 | mul 1000 -}}
{{- else if hasSuffix "M" $quantity -}}
{{- $n = trimSuffix "M" $quantity | int64 | mul 1000000 -}}
{{- else if hasSuffix "G" $quantity -}}
{{- $n = trimSuffix "G" $quantity | int64 | mul 1000000000 -}}
{{- else if hasSuffix "T" $quantity -}}
{{- $n = trimSuffix "T" $quantity | int64 | mul 1000000000000 -}}
{{- else if hasSuffix "P" $quantity -}}
{{- $n = trimSuffix "P" $quantity | int64 | mul 1000000000000000 -}}
{{- else if hasSuffix "E" $quantity -}}
{{- $n = trimSuffix "E" $quantity | int64 | mul 1000000000000000000 -}}
{{- else if hasSuffix "ki" $quantity -}}
{{- $n = trimSuffix "ki" $quantity | int64 | mul 1024 -}}
{{- else if hasSuffix "Mi" $quantity -}}
{{- $n = trimSuffix "Mi" $quantity | int64 | mul 1048576 -}}
{{- else if hasSuffix "Gi" $quantity -}}
{{- $n = trimSuffix "Gi" $quantity | int64 | mul 1073741824 -}}
{{- else if hasSuffix "Ti" $quantity -}}
{{- $n = trimSuffix "Ti" $quantity | int64 | mul 1099511627776 -}}
{{- else if hasSuffix "Pi" $quantity -}}
{{- $n = trimSuffix "Pi" $quantity | int64 | mul 1125899906842624 -}}
{{- else if hasSuffix "Ei" $quantity -}}
{{- $n = trimSuffix "Ei" $quantity | int64 | mul 1152921504606846976 -}}
{{- end -}}
{{- end -}}
{{- if le ($n | float64) 0.0 -}}
{{- fail (print "invalid quantity: " $quantity) -}}
{{- end -}}
{{- if kindIs "float64" $n -}}
{{- printf "%f" $n -}}
{{- else -}}
{{- printf "%v" $n -}}
{{- end -}}
{{- end -}}

