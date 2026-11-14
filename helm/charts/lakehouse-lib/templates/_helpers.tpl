{{/* Shared helpers; selectors stay `app: <name>` because Deployment/StatefulSet selectors are immutable. */}}

{{- define "lakehouse.selectorLabels" -}}
app: {{ .name }}
{{- end }}

{{/* Version-free labels for resources whose labels an operator copies onto pods (a version label would roll every pod). */}}
{{- define "lakehouse.stableLabels" -}}
{{ include "lakehouse.selectorLabels" . }}
app.kubernetes.io/name: {{ .name }}
app.kubernetes.io/instance: {{ .root.Release.Name }}
app.kubernetes.io/part-of: lakehouse
app.kubernetes.io/managed-by: {{ .root.Release.Service }}
lakehouse.io/environment: {{ .root.Values.global.environment | default "local" }}
{{- end }}

{{- define "lakehouse.labels" -}}
{{ include "lakehouse.stableLabels" . }}
helm.sh/chart: {{ printf "%s-%s" .root.Chart.Name .root.Chart.Version | replace "+" "_" }}
{{- end }}

{{/* Full image reference: optional global registry + repository + tag (component tag, else global tag). */}}
{{- define "lakehouse.image" -}}
{{- $registry := .root.Values.global.imageRegistry | default "" -}}
{{- $tag := .image.tag | default .root.Values.global.imageTag | default "dev" -}}
{{- if and $registry (not .image.external) -}}
{{ printf "%s/%s:%s" $registry .image.repository $tag }}
{{- else -}}
{{ printf "%s:%s" .image.repository $tag }}
{{- end -}}
{{- end }}

{{- define "lakehouse.imagePullPolicy" -}}
{{ .root.Values.global.imagePullPolicy | default "IfNotPresent" }}
{{- end }}

{{/* Pod-level security context for our own images (non-root, default seccomp). */}}
{{- define "lakehouse.podSecurityContext" -}}
runAsNonRoot: true
runAsUser: {{ .uid | default 10001 }}
seccompProfile:
  type: RuntimeDefault
{{- end }}

{{/* Pod Security "restricted" container settings; "writableRoot" true for images that write to their install dir (Flink, Spark). */}}
{{- define "lakehouse.containerSecurityContext" -}}
allowPrivilegeEscalation: false
readOnlyRootFilesystem: {{ not .writableRoot }}
capabilities:
  drop: ["ALL"]
{{- end }}

{{/* S3 env vars: endpoint and static keys locally; nothing on AWS, where IRSA applies. */}}
{{- define "lakehouse.s3Env" -}}
{{- $s3 := .root.Values.global.s3 | default dict -}}
{{- if $s3.endpoint }}
- name: S3_ENDPOINT
  value: {{ $s3.endpoint | quote }}
- name: S3_ACCESS_KEY_ID
  valueFrom:
    secretKeyRef:
      name: {{ $s3.credentialsSecret | default "s3-credentials" }}
      key: AWS_ACCESS_KEY_ID
- name: S3_SECRET_ACCESS_KEY
  valueFrom:
    secretKeyRef:
      name: {{ $s3.credentialsSecret | default "s3-credentials" }}
      key: AWS_SECRET_ACCESS_KEY
{{- end }}
- name: AWS_REGION
  value: {{ .root.Values.global.aws.region | default "us-east-1" | quote }}
{{- end }}

{{/* Iceberg catalog location shared by the Flink and Spark jobs. */}}
{{- define "lakehouse.icebergEnv" -}}
- name: ICEBERG_CATALOG_TYPE
  value: {{ .root.Values.global.iceberg.catalogType | default "rest" | quote }}
- name: ICEBERG_REST_URI
  value: {{ .root.Values.global.iceberg.restUri | default "" | quote }}
- name: ICEBERG_WAREHOUSE
  value: {{ .root.Values.global.iceberg.warehouse | quote }}
{{- end }}

{{/* Kafka client mTLS from the KafkaUser and CA secrets Kyverno clones in; args (dict "root" $ "prefix" "<ENV_PREFIX>_" "user" "<KafkaUser>"). */}}
{{- define "lakehouse.kafkaClientEnv" -}}
{{- $tls := .root.Values.global.kafka.tls | default dict -}}
{{- if $tls.enabled }}
- name: {{ .prefix }}SECURITY_PROTOCOL
  value: SSL
- name: {{ .prefix }}SSL_KEYSTORE_TYPE
  value: PKCS12
- name: {{ .prefix }}SSL_KEYSTORE_LOCATION
  value: /etc/kafka-client/user/user.p12
- name: {{ .prefix }}SSL_KEYSTORE_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ .user }}
      key: user.password
- name: {{ .prefix }}SSL_TRUSTSTORE_TYPE
  value: PKCS12
- name: {{ .prefix }}SSL_TRUSTSTORE_LOCATION
  value: /etc/kafka-client/ca/ca.p12
- name: {{ .prefix }}SSL_TRUSTSTORE_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ $tls.caSecret }}
      key: ca.password
{{- end }}
{{- end }}

{{- define "lakehouse.kafkaClientVolumeMounts" -}}
{{- $tls := .root.Values.global.kafka.tls | default dict -}}
{{- if $tls.enabled }}
- name: kafka-client-user
  mountPath: /etc/kafka-client/user
  readOnly: true
- name: kafka-client-ca
  mountPath: /etc/kafka-client/ca
  readOnly: true
{{- end }}
{{- end }}

{{- define "lakehouse.kafkaClientVolumes" -}}
{{- $tls := .root.Values.global.kafka.tls | default dict -}}
{{- if $tls.enabled }}
- name: kafka-client-user
  secret:
    secretName: {{ .user }}
- name: kafka-client-ca
  secret:
    secretName: {{ $tls.caSecret }}
{{- end }}
{{- end }}

{{/* Fails rendering early with a clear message when a required value is missing. */}}
{{- define "lakehouse.required" -}}
{{- if not .value -}}
{{- fail (printf "%s is required (environment %s)" .key (.root.Values.global.environment | default "local")) -}}
{{- end -}}
{{- end }}
