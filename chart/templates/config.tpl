{{- define "gateway_config.yaml" }}
logs_collector_id: {{ .Values.network.logs_collector_peer_id | quote }}
send_metrics: true
worker_inactive_threshold_sec: 60
worker_greylist_time_sec: 120
default_query_timeout_sec: 60
summary_print_interval_sec: 0
workers_update_interval_sec: 60
available_datasets:
{{- range $name, $dataset := .Values.datasets }}
    {{ $name }}: {{ $dataset }}
{{- end }}
{{- end }}
