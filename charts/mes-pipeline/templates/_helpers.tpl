{{/* charts/mes-pipeline/templates/_helpers.tpl */}}

{{/*
Baut den vollen Image-Namen fuer die VIER EIGENEN, im Team gebauten Images
(ingestion, stream-processing, serving-api, ui). Kafka und MinIO sind
Fremd-Images und rufen das NICHT auf - die haben ihr eigenes, festes
Values.<component>.image, siehe deren Templates.
*/}}
{{- define "mes.image" -}}
{{- $registry := .ctx.Values.global.imageRegistry -}}
{{- if $registry -}}
{{ $registry }}{{ .image }}:{{ .tag }}
{{- else -}}
{{ .image }}:{{ .tag }}
{{- end -}}
{{- end -}}

{{/*
Gemeinsame Selector-Labels fuer eine Komponente.
*/}}
{{- define "mes.selectorLabels" -}}
app: {{ .component }}
app.kubernetes.io/instance: {{ .ctx.Release.Name }}
{{- end -}}
