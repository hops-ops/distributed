{{/* The checkout root (source.localPath is examples/game). */}}
{{- define "mobpit.repo" -}}
{{- .Values.source.localPath | dir | dir -}}
{{- end -}}

{{- define "mobpit.game" -}}
{{- .Values.source.localPath -}}
{{- end -}}

{{- define "mobpit.host" -}}
{{- printf "%s.%s" .Values.environment.name .Values.localDomain -}}
{{- end -}}

{{- define "mobpit.relayUrl" -}}
{{- printf "http://mobpit.%s.svc.cluster.local:2567/internal/celld-queue/relay" .Values.environment.namespace -}}
{{- end -}}

{{- define "mobpit.labels" -}}
app.kubernetes.io/part-of: mobpit
hops.ops.com.ai/runtime: cluster-dev
{{- end -}}
