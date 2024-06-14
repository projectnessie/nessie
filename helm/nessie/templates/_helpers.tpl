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
Apply Iceberg catalog options.
*/}}
{{- define "nessie.applyCatalogIcebergOptions" -}}
{{- $root := index . 0 -}}{{/* the object to introspect */}}
{{- $map := index . 1 -}}{{/* the destination map */}}
{{- with $root -}}
{{- $_ := set $map "nessie.catalog.default-warehouse" .defaultWarehouse -}}
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
{{- define "nessie.applyCatalogStorageS3Options" -}}
{{- $root := index . 0 -}}{{/* the object to introspect */}}
{{- $prefix := index . 1 -}}{{/* the current prefix */}}
{{- $map := index . 2 -}}{{/* the destination map */}}
{{- with $root -}}
{{- if .region -}}{{- $_ := set $map ( print $prefix "region" ) .region -}}{{- end -}}
{{- if .endpoint -}}{{- $_ := set $map ( print $prefix "endpoint" ) .endpoint -}}{{- end -}}
{{- if .externalEndpoint -}}{{- $_ := set $map ( print $prefix "external-endpoint" ) .externalEndpoint -}}{{- end -}}
{{- if .pathStyleAccess -}}{{- $_ := set $map ( print $prefix "path-style-access" ) .pathStyleAccess -}}{{- end -}}
{{- if .accessPoint -}}{{- $_ := set $map ( print $prefix "access-point" ) .accessPoint -}}{{- end -}}
{{- if .allowCrossRegionAccessPoint -}}{{- $_ := set $map ( print $prefix "allow-cross-region-access-point" ) .allowCrossRegionAccessPoint -}}{{- end -}}
{{- if .clientAuthenticationMode -}}{{- $_ := set $map ( print $prefix "auth-mode" ) .clientAuthenticationMode -}}{{- end -}}
{{- if .assumeRole -}}
{{- if .assumeRole.stsEndpoint -}}{{- $_ := set $map ( print $prefix "sts.endpoint" ) .assumeRole.stsEndpoint -}}{{- end -}}
{{- if .assumeRole.roleArn -}}{{- $_ := set $map ( print $prefix "assumed-role" ) .assumeRole.roleArn -}}{{- end -}}
{{- if .assumeRole.sessionIamPolicy -}}{{- $_ := set $map ( print $prefix "session-iam-policy" ) .assumeRole.sessionIamPolicy -}}{{- end -}}
{{- if .assumeRole.roleSessionName -}}{{- $_ := set $map ( print $prefix "role-session-name" ) .assumeRole.roleSessionName -}}{{- end -}}
{{- if .assumeRole.externalId -}}{{- $_ := set $map ( print $prefix "external-id" ) .assumeRole.externalId -}}{{- end -}}
{{- if .assumeRole.clientSessionDuration -}}{{- $_ := set $map ( print $prefix "client-session-duration" ) .assumeRole.clientSessionDuration -}}{{- end -}}
{{- if .assumeRole.sessionCredentialRefreshGracePeriod -}}{{- $_ := set $map ( print $prefix "sts.session-grace-period" ) .assumeRole.sessionCredentialRefreshGracePeriod -}}{{- end -}}
{{- if .assumeRole.sessionCredentialCacheMaxEntries -}}{{- $_ := set $map ( print $prefix "sts.session-cache-max-size" ) .assumeRole.sessionCredentialCacheMaxEntries -}}{{- end -}}
{{- if .assumeRole.stsClientsCacheMaxEntries -}}{{- $_ := set $map ( print $prefix "sts.clients-cache-max-size" ) .assumeRole.stsClientsCacheMaxEntries -}}{{- end -}}
{{- end -}}
{{- if .transport -}}
{{- if .transport.maxHttpConnections -}}{{- $_ := set $map ( print $prefix "http.max-http-connections" ) .transport.maxHttpConnections -}}{{- end -}}
{{- if .transport.readTimeout -}}{{- $_ := set $map ( print $prefix "http.read-timeout" ) .transport.readTimeout -}}{{- end -}}
{{- if .transport.connectTimeout -}}{{- $_ := set $map ( print $prefix "http.connect-timeout" ) .transport.connectTimeout -}}{{- end -}}
{{- if .transport.connectionAcquisitionTimeout -}}{{- $_ := set $map ( print $prefix "http.connection-acquisition-timeout" ) .transport.connectionAcquisitionTimeout -}}{{- end -}}
{{- if .transport.connectionMaxIdleTime -}}{{- $_ := set $map ( print $prefix "http.connection-max-idle-time" ) .transport.connectionMaxIdleTime -}}{{- end -}}
{{- if .transport.connectionTimeToLive -}}{{- $_ := set $map ( print $prefix "http.connection-time-to-live" ) .transport.connectionTimeToLive -}}{{- end -}}
{{- if .transport.expectContinueEnabled -}}{{- $_ := set $map ( print $prefix "http.expect-continue-enabled" ) .transport.expectContinueEnabled -}}{{- end -}}
{{- if .transport.retryAfter -}}{{- $_ := set $map ( print $prefix "throttled-retry-after" ) .transport.retryAfter -}}{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Apply GCS catalog options.
*/}}
{{- define "nessie.applyCatalogStorageGcsOptions" -}}
{{- $root := index . 0 -}}{{/* the object to introspect */}}
{{- $prefix := index . 1 -}}{{/* the current prefix */}}
{{- $map := index . 2 -}}{{/* the destination map */}}
{{- with $root -}}
{{- if .host -}}{{- $_ := set $map ( print $prefix "host" ) .host -}}{{- end -}}
{{- if .externalHost -}}{{- $_ := set $map ( print $prefix "external-host" ) .externalHost -}}{{- end -}}
{{- if .userProject -}}{{- $_ := set $map ( print $prefix "user-project" ) .userProject -}}{{- end -}}
{{- if .projectId -}}{{- $_ := set $map ( print $prefix "project-id" ) .projectId -}}{{- end -}}
{{- if .quotaProjectId -}}{{- $_ := set $map ( print $prefix "quota-project-id" ) .quotaProjectId -}}{{- end -}}
{{- if .clientLibToken -}}{{- $_ := set $map ( print $prefix "client-lib-token" ) .clientLibToken -}}{{- end -}}
{{- if .authType -}}{{- $_ := set $map ( print $prefix "auth-type" ) .authType -}}{{- end -}}
{{- if .encryptionKey -}}{{- $_ := set $map ( print $prefix "encryption-key" ) .encryptionKey -}}{{- end -}}
{{- if .decryptionKey -}}{{- $_ := set $map ( print $prefix "decryption-key" ) .decryptionKey -}}{{- end -}}
{{- if .transport -}}
{{- if .transport.maxAttempts -}}{{- $_ := set $map ( print $prefix "max-attempts" ) .transport.maxAttempts -}}{{- end -}}
{{- if .transport.connectTimeout -}}{{- $_ := set $map ( print $prefix "connect-timeout" ) .transport.connectTimeout -}}{{- end -}}
{{- if .transport.readTimeout -}}{{- $_ := set $map ( print $prefix "read-timeout" ) .transport.readTimeout -}}{{- end -}}
{{- if .transport.initialRetryDelay -}}{{- $_ := set $map ( print $prefix "initial-retry-delay" ) .transport.initialRetryDelay -}}{{- end -}}
{{- if .transport.maxRetryDelay -}}{{- $_ := set $map ( print $prefix "max-retry-delay" ) .transport.maxRetryDelay -}}{{- end -}}
{{- if .transport.retryDelayMultiplier -}}{{- $_ := set $map ( print $prefix "retry-delay-multiplier" ) .transport.retryDelayMultiplier -}}{{- end -}}
{{- if .transport.initialRpcTimeout -}}{{- $_ := set $map ( print $prefix "initial-rpc-timeout" ) .transport.initialRpcTimeout -}}{{- end -}}
{{- if .transport.maxRpcTimeout -}}{{- $_ := set $map ( print $prefix "max-rpc-timeout" ) .transport.maxRpcTimeout -}}{{- end -}}
{{- if .transport.rpcTimeoutMultiplier -}}{{- $_ := set $map ( print $prefix "rpc-timeout-multiplier" ) .transport.rpcTimeoutMultiplier -}}{{- end -}}
{{- if .transport.logicalTimeout -}}{{- $_ := set $map ( print $prefix "logical-timeout" ) .transport.logicalTimeout -}}{{- end -}}
{{- if .transport.totalTimeout -}}{{- $_ := set $map ( print $prefix "total-timeout" ) .transport.totalTimeout -}}{{- end -}}
{{- if .transport.readChunkSize -}}{{- $_ := set $map ( print $prefix "read-chunk-size" ) .transport.readChunkSize -}}{{- end -}}
{{- if .transport.writeChunkSize -}}{{- $_ := set $map ( print $prefix "write-chunk-size" ) .transport.writeChunkSize -}}{{- end -}}
{{- if .transport.deleteBatchSize -}}{{- $_ := set $map ( print $prefix "delete-batch-size" ) .transport.deleteBatchSize -}}{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Apply ADLS catalog options.
*/}}
{{- define "nessie.applyCatalogStorageAdlsOptions" -}}
{{- $root := index . 0 -}}{{/* the object to introspect */}}
{{- $prefix := index . 1 -}}{{/* the current prefix */}}
{{- $map := index . 2 -}}{{/* the destination map */}}
{{- with $root -}}
{{- if .endpoint -}}{{- $_ := set $map ( print $prefix "endpoint" ) .endpoint -}}{{- end -}}
{{- if .externalEndpoint -}}{{- $_ := set $map ( print $prefix "external-endpoint" ) .externalEndpoint -}}{{- end -}}
{{- if .transport -}}
{{- if .transport.retryPolicy -}}{{- $_ := set $map ( print $prefix "retry-policy" ) .transport.retryPolicy -}}{{- end -}}
{{- if .transport.maxRetries -}}{{- $_ := set $map ( print $prefix "max-retries" ) .transport.maxRetries -}}{{- end -}}
{{- if .transport.tryTimeout -}}{{- $_ := set $map ( print $prefix "try-timeout" ) .transport.tryTimeout -}}{{- end -}}
{{- if .transport.retryDelay -}}{{- $_ := set $map ( print $prefix "retry-delay" ) .transport.retryDelay -}}{{- end -}}
{{- if .transport.maxRetryDelay -}}{{- $_ := set $map ( print $prefix "max-retry-delay" ) .transport.maxRetryDelay -}}{{- end -}}
{{- if .transport.maxHttpConnections -}}{{- $_ := set $map ( print $prefix "max-http-connections" ) .transport.maxHttpConnections -}}{{- end -}}
{{- if .transport.connectTimeout -}}{{- $_ := set $map ( print $prefix "connect-timeout" ) .transport.connectTimeout -}}{{- end -}}
{{- if .transport.readTimeout -}}{{- $_ := set $map ( print $prefix "read-timeout" ) .transport.readTimeout -}}{{- end -}}
{{- if .transport.writeTimeout -}}{{- $_ := set $map ( print $prefix "write-timeout" ) .transport.writeTimeout -}}{{- end -}}
{{- if .transport.connectionIdleTimeout -}}{{- $_ := set $map ( print $prefix "connection-idle-timeout" ) .transport.connectionIdleTimeout -}}{{- end -}}
{{- if .transport.readBlockSize -}}{{- $_ := set $map ( print $prefix "read-block-size" ) .transport.readBlockSize -}}{{- end -}}
{{- if .transport.writeBlockSize -}}{{- $_ := set $map ( print $prefix "write-block-size" ) .transport.writeBlockSize -}}{{- end -}}
{{- end -}}
{{- list .advancedConfig ( print $prefix "configuration" ) $map | include "nessie.mergeAdvancedConfig" }}
{{- end -}}
{{- end -}}
