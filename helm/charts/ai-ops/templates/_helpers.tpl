{{/* One env var from a Secret key (key defaults to the env name). */}}
{{- define "ai-ops.secretEnv" -}}
- name: {{ .env }}
  valueFrom:
    secretKeyRef: {name: {{ .secret }}, key: {{ .key | default .env }}{{ if .optional }}, optional: true{{ end }}}
{{- end }}
