.PHONY: dev1 start dev2 dev3 remsh

dev1:
	export FAXE_DFS_SCRIPT_PATH=/home/heyoka/workspace/faxe/dfs/ && \
#	export FAXE_FLOW_AUTO_START=off && \
	export FAXE_ALLOW_ANONYMOUS=true && \
#	export FAXE_S7POOL_MIN_SIZE=1 && \
#	export FAXE_CONN_STATUS_HANDLER_MQTT_PORT=9876 && \
	export FAXE_HTTP_API_TLS_ENABLE=on && \
	export FAXE_HTTP_API_SSL_CERTFILE=/home/heyoka/workspace/faxe/certs_tgw/tgw_wildcard.crt && \
	export FAXE_HTTP_API_SSL_CACERTFILE=/home/heyoka/workspace/faxe/certs_tgw/tgw_wildcard.crt && \
	export FAXE_HTTP_API_SSL_KEYFILE=/home/heyoka/workspace/faxe/certs_tgw/tgw_wildcard.key && \
	export FAXE_CONN_STATUS_HANDLER_MQTT_ENABLE=on && \
	export FAXE_METRICS_HANDLER_MQTT_ENABLE=off && \
#	export FAXE_DEBUG_HANDLER_MQTT_HOST=10.156.15.11 && \
	export FAXE_DEBUG_HANDLER_MQTT_ENABLE=on && \
	rebar3 as dev1 release && _build/dev1/rel/faxe/bin/faxe console


start:
	export FAXE_DFS_SCRIPT_PATH=/home/heyoka/workspace/faxe/dfs/ && \
    #	export FAXE_FLOW_AUTO_START=off && \
    	export FAXE_ALLOW_ANONYMOUS=true && \
  export FAXE_EMAIL_PASS='34joijr2#zzub#fe553' && \
    #	export FAXE_CONN_STATUS_HANDLER_MQTT_PORT=9876 && \ 
    	export FAXE_CONN_STATUS_HANDLER_MQTT_ENABLE=off && \
    	export FAXE_METRICS_HANDLER_MQTT_ENABLE=off && \
    	export FAXE_DEBUG_HANDLER_MQTT_ENABLE=off && \
	_build/dev1/rel/faxe/bin/faxe console

dev2:
	rebar3 as dev2 release && _build/dev2/rel/faxe/bin/faxe console

dev3:
	rebar3 as dev3 release && _build/dev3/rel/faxe/bin/faxe console

dev4:
	rebar3 as dev4 release && _build/dev4/rel/faxe/bin/faxe console

dev5:
	rebar3 as dev5 release && _build/dev5/rel/faxe/bin/faxe console

rmsh:
	erl -sname rm -remsh faxe1@ubuntu -setcookie distrepl_proc_cookie