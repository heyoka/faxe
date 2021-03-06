%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 01. Sep 2020 09:52
%%%-------------------------------------------------------------------
-module(dfs_to_graph).
-author("heyoka").

-include_lib("eunit/include/eunit.hrl").

compile_helper(DfsScriptFile) ->
  {_, GraphMap} = faxe_dfs:file(DfsScriptFile, []),
  GraphMap.

unknown_opt_test() ->
  application:set_env(faxe, dfs, [{script_path, "apps/faxe/test/dfs/"}]),
  Expected = {error,"Unknown option 'ls_mem' for node 'debug'"},
  ?assertEqual(Expected, compile_helper("unknown_options_test.dfs")).

batch_test() ->
  Expected =  #{edges =>
  [{<<"value_emitter1">>,1,<<"batch2">>,1,[]},
    {<<"batch2">>,1,<<"debug3">>,1,[]}],
    nodes =>
    [{<<"debug3">>,esp_debug,#{level => <<"warning">>}},
      {<<"batch2">>,esp_batch,
        #{size => 5,timeout => <<"5750ms">>}},
      {<<"value_emitter1">>,esp_value_emitter,
        #{align => false,batch_size => 5,
          every => <<"8000ms">>,
          fields => [<<"val">>],
          format => undefined,jitter => <<"3700ms">>,
          type => point}}]}
  ,
  ?assertEqual(Expected, compile_helper("batch_test.dfs")).



bridge_test() ->
  Expected =  #{edges =>
  [{<<"debug2">>,1,<<"amqp_publish5">>,1,[]},
    {<<"debug2">>,1,<<"amqp_publish4">>,1,[]},
    {<<"debug2">>,1,<<"amqp_publish3">>,1,[]},
    {<<"mqtt_subscribe1">>,1,<<"debug2">>,1,[]}],
    nodes =>
    [{<<"amqp_publish5">>,esp_amqp_publish,
      #{exchange => <<"x_root_fanout">>,
        host => <<"15.45.48.1">>,
        pass => <<"dfwefwef8ePI78we">>,port => undefined,
        routing_key => <<"some.crazy.topic.this.is">>,
        routing_key_lambda => undefined,safe => false,
        ssl => false,user => <<"rabbitmq-cluster-user">>,
        vhost => <<"/">>}},
      {<<"amqp_publish4">>,esp_amqp_publish,
        #{exchange => <<"x_root_fanout">>,
          host => <<"some.other_amqp_host">>,
          pass => <<"adfafdwewef3">>,port => undefined,
          routing_key => <<"some.crazy.topic.this.is">>,
          routing_key_lambda => undefined,safe => false,
          ssl => false,user => <<"rabbitmq-cluster-user">>,
          vhost => <<"/">>}},
      {<<"amqp_publish3">>,esp_amqp_publish,
        #{exchange => <<"x_root_fanout">>,
          host => <<"some.amqp_host">>,
          pass => <<"asdf323232">>,port => undefined,
          routing_key => <<"some.crazy.topic.this.is">>,
          routing_key_lambda => undefined,safe => false,
          ssl => false,user => <<"rabbitmq-cluster-user">>,
          vhost => <<"/">>}},
      {<<"debug2">>,esp_debug,#{level => <<"notice">>}},
      {<<"mqtt_subscribe1">>,esp_mqtt_subscribe,
        #{as => undefined,dt_field => <<"ts">>,
          dt_format => <<"millisecond">>,
          host => <<"10.102.1.102">>,include_topic => true,
          pass => undefined,port => 1883,qos => 1,
          ssl => false,
          topic => <<"some/crazy/topic/this/is">>,
          topic_as => <<"topic">>,topics => undefined,
          user => undefined}}]}

  ,
  ?assertEqual(Expected, compile_helper("mqtt_amqp_bridge_test.dfs")).

