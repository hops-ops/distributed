{{- define "e2e-ui-api.name" -}}
{{- default .Chart.Name .Values.name | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "e2e-ui-api.releaseNamespace" -}}
{{- coalesce .Values.environment.namespace .Release.Namespace "default" -}}
{{- end -}}

{{- define "e2e-ui-api.labels" -}}
app.kubernetes.io/name: {{ include "e2e-ui-api.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "e2e-ui-api.workspace" -}}
{{- if .Values.identity.workspace -}}
{{- .Values.identity.workspace -}}
{{- else -}}
{{- coalesce .Values.environment.name .Values.environment.namespace .Release.Namespace "default" -}}
{{- end -}}
{{- end -}}

{{- define "e2e-ui-api.oidcConnectionSecretName" -}}
{{- if .Values.identity.connectionSecretName -}}
{{- .Values.identity.connectionSecretName | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $prefix := printf "e2e-ui-%s" (include "e2e-ui-api.workspace" .) -}}
{{- $generation := int (.Values.identity.oidcGeneration | default 0) -}}
{{- if gt $generation 0 -}}
{{- printf "%s-oidc-conn-g%d" $prefix $generation | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-oidc-conn" $prefix | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "e2e-ui-api.oidcClientEnv" -}}
{{- if .Values.identity.enabled }}
{{- $secret := include "e2e-ui-api.oidcConnectionSecretName" . }}
- name: OIDC_AUDIENCE
  valueFrom:
    secretKeyRef:
      name: {{ $secret | quote }}
      key: attribute.client_id
- name: OIDC_CLIENT_ID
  valueFrom:
    secretKeyRef:
      name: {{ $secret | quote }}
      key: attribute.client_id
{{- end }}
{{- end -}}

{{/*
Host directory of the checkout that contains this fixture: the injected
sourceDelivery.hostPath (the Environment root) without checkoutSubpath.
*/}}
{{- define "e2e-ui-api.sourceCheckoutPath" -}}
{{- $sd := .Values.sourceDelivery -}}
{{- $sub := trimAll "/" (toString $sd.checkoutSubpath) -}}
{{- if not $sub -}}
{{- fail "sourceDelivery.checkoutSubpath must name this fixture's path in the checkout" -}}
{{- end -}}
{{- $workingDir := printf "%s/%s" (trimSuffix "/" $sd.mountPath) $sub -}}
{{- if ne .Values.clusterDev.workingDir $workingDir -}}
{{- fail (printf "clusterDev.workingDir %q must be %q (mountPath/checkoutSubpath)" .Values.clusterDev.workingDir $workingDir) -}}
{{- end -}}
{{- $host := trimSuffix "/" (toString $sd.hostPath) -}}
{{- $suffix := printf "/%s" $sub -}}
{{- $checkout := trimSuffix $suffix $host -}}
{{- if or (not (hasSuffix $suffix $host)) (not $checkout) -}}
{{- fail (printf "sourceDelivery.hostPath %q must be a directory ending in %q (the Environment root)" $host $suffix) -}}
{{- end -}}
{{- $checkout -}}
{{- end -}}
