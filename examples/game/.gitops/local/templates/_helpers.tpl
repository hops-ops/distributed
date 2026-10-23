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
{{- printf "https://mobpit-relay.%s.svc.cluster.local:8443/internal/celld-queue/relay" .Values.environment.namespace -}}
{{- end -}}

{{- define "mobpit.labels" -}}
app.kubernetes.io/part-of: mobpit
hops.ops.com.ai/runtime: cluster-dev
{{- end -}}
