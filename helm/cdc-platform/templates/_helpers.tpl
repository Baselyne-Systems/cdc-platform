{{/*
Expand the name of the chart.
*/}}
{{- define "cdc-platform.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "cdc-platform.fullname" -}}
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
Common labels.
*/}}
{{- define "cdc-platform.labels" -}}
helm.sh/chart: {{ include "cdc-platform.name" . }}-{{ .Chart.Version | replace "+" "_" }}
{{ include "cdc-platform.selectorLabels" . }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels.
*/}}
{{- define "cdc-platform.selectorLabels" -}}
app.kubernetes.io/name: {{ include "cdc-platform.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Kafka Connect selector labels.
*/}}
{{- define "cdc-platform.connectSelectorLabels" -}}
app.kubernetes.io/name: {{ include "cdc-platform.name" . }}-connect
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Kafka bootstrap servers — use sub-chart service or user-supplied value.
*/}}
{{- define "cdc-platform.kafkaBootstrapServers" -}}
{{- if .Values.kafka.enabled -}}
{{ include "cdc-platform.fullname" . }}-kafka:9092
{{- else -}}
{{ .Values.kafka.externalBootstrapServers | default "localhost:9092" }}
{{- end -}}
{{- end }}

{{/*
Schema Registry URL — use sub-chart service or user-supplied value.
*/}}
{{- define "cdc-platform.schemaRegistryUrl" -}}
{{- if .Values.schemaRegistry.enabled -}}
http://{{ include "cdc-platform.fullname" . }}-schema-registry:8081
{{- else -}}
{{ .Values.schemaRegistry.externalUrl | default "http://localhost:8081" }}
{{- end -}}
{{- end }}

{{/*
Check if transport mode is Kafka.
*/}}
{{- define "cdc-platform.isKafkaTransport" -}}
{{- eq (.Values.platform.transportMode | default "kafka") "kafka" -}}
{{- end }}
