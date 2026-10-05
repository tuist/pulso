{{- define "pulso.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "pulso.fullname" -}}
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

{{- define "pulso.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "pulso.selectorLabels" -}}
app.kubernetes.io/name: {{ include "pulso.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "pulso.labels" -}}
helm.sh/chart: {{ include "pulso.chart" . }}
{{ include "pulso.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "pulso.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "pulso.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{- define "pulso.secretName" -}}
{{- default (include "pulso.fullname" .) .Values.existingSecret }}
{{- end }}

{{/*
Fails rendering early with a readable message instead of letting the release
crash on boot.
*/}}
{{- define "pulso.validate" -}}
{{- if not .Values.storage.bucket }}
{{- fail "storage.bucket is required" }}
{{- end }}
{{- if not .Values.storage.region }}
{{- fail "storage.region is required" }}
{{- end }}
{{- $knownLimits := list "max_records" "max_attributes" "max_key_bytes" "max_value_bytes" "max_attribute_bytes" "max_depth" "max_nodes" }}
{{- range $key, $value := .Values.ingestLimits }}
{{- if not (has $key $knownLimits) }}
{{- fail (printf "ingestLimits: unknown setting %q (expected one of %s)" $key (join ", " $knownLimits)) }}
{{- end }}
{{- end }}
{{- if not .Values.existingSecret }}
{{- if not .Values.tenantTokens }}
{{- fail "tenantTokens needs at least one tenant (or set existingSecret)" }}
{{- end }}
{{- range $tenant, $digest := .Values.tenantTokens }}
{{- if not (regexMatch "^[A-Za-z0-9_.-]{1,128}$" $tenant) }}
{{- fail (printf "tenantTokens: %q is not a valid tenant name (letters, digits, '_', '.', '-', at most 128)" $tenant) }}
{{- end }}
{{- if not (regexMatch "^sha256\\$[0-9a-f]{64}$" (toString $digest)) }}
{{- fail (printf "tenantTokens.%s must be \"sha256$\" followed by 64 lowercase hex characters (a digest, not the token)" $tenant) }}
{{- end }}
{{- end }}
{{- if or (not .Values.storage.accessKeyId) (not .Values.storage.secretAccessKey) }}
{{- fail "storage.accessKeyId and storage.secretAccessKey are required (or set existingSecret)" }}
{{- end }}
{{- end }}
{{- end }}
