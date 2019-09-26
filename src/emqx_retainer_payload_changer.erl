%%%-------------------------------------------------------------------
%%% @author jdavidagudelo
%%% @copyright (C) 2019, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 19. Sep 2019 1:20 p. m.
%%%-------------------------------------------------------------------
-module(emqx_retainer_payload_changer).
-author("jdavidagudelo").
-include_lib("eunit/include/eunit.hrl").

%% API
-export([get_retained_messages_from_topic/1]).

get_file_path_local(LocalFilePath) ->
  {ok, FilePath} = file:get_cwd(),
  filename:join([FilePath, LocalFilePath]).

get_retainer_configuration() ->
  {ok, FilePath} = file:get_cwd(),
  FilePathConfiguration = filename:join([FilePath, "retainer_changer", "retainer_changer.conf"]),
  {ok, [Options|_]} = file:consult(FilePathConfiguration),
  Options.


get_reactor_redis_client(Options) ->
  Host = maps:get(reactor_cache_host_name, Options, "127.0.0.1"),
  Port = maps:get(reactor_cache_port, Options, 6379),
  Database = maps:get(reactor_cache_database, Options, 1),
  Password = maps:get(reactor_cache_password, Options, ""),
  {ok, RedisClient} = eredis:start_link(Host, Port, Database, Password, no_reconnect),
  RedisClient.

get_ubidots_redis_client(Options) ->
  Host = maps:get(ubidots_cache_host_name, Options, "127.0.0.1"),
  Port = maps:get(ubidots_cache_port, Options, 6379),
  Database = maps:get(ubidots_cache_database, Options, 1),
  Password = maps:get(ubidots_cache_password, Options, ""),
  {ok, RedisClient} = eredis:start_link(Host, Port, Database, Password, no_reconnect),
  RedisClient.

get_lua_script_from_file(FilePath) ->
  {ok, FileData} = file:read_file(FilePath),
  FileData.

get_variables_from_topic(RedisClient, ScriptData, Topic) ->
  {ok, Result} = eredis:q(RedisClient, ["EVAL", ScriptData, 1, Topic]),
  Result.

get_values_variables(RedisClient, ScriptData, VariablesData) ->
  VariablesDataArray = array:from_list(VariablesData),
  Args = ["EVAL", ScriptData, array:size(VariablesDataArray)] ++ VariablesData,
  {ok, Result} = eredis:q(RedisClient, Args),
  Result.

get_values_from_topic(Topic) ->
  Options = get_retainer_configuration(),
  ReactorScriptFilePath = maps:get(reactor_cache_get_subscription_variables_from_mqtt_topic_script_file_path, Options, ""),
  UbidotsScriptFilePath = maps:get(ubidots_cache_get_values_variables_script_file_path, Options, ""),
  ReactorRedisClient = get_reactor_redis_client(Options),
  UbidotsRedisClient = get_ubidots_redis_client(Options),
  ReactorScriptData = get_lua_script_from_file(get_file_path_local(ReactorScriptFilePath)),
  UbidotsScriptData = get_lua_script_from_file(get_file_path_local(UbidotsScriptFilePath)),
  VariablesData = get_variables_from_topic(ReactorRedisClient, ReactorScriptData, Topic),
  Values = get_values_variables(UbidotsRedisClient, UbidotsScriptData, VariablesData),
  Values.

get_messages([]) ->
  [];
get_messages([Topic, Value|Rest]) ->
  NewMessage = emqx_message:make(Topic, Value),
  [NewMessage | get_messages(Rest)].

get_retained_messages_from_topic(Topic) ->
  Values = get_values_from_topic(Topic),
  get_messages(Values).


initialize_variables(_, _, _, []) ->
  ok;
initialize_variables(RedisClient, OwnerId, DeviceLabel, [VariableLabel, VariableId | Rest]) ->
  eredis:q(RedisClient, ["HSET", "reactor_variable_data/" ++ VariableId, "/variable_label", VariableLabel]),
  eredis:q(RedisClient, ["HSET", "reactor_variables/" ++ OwnerId, "/" ++ DeviceLabel ++ "/" ++ VariableLabel, VariableId]),
  ValueData = "{\"value\": 11.1, \"timestamp\": 11, \"context\": {\"a\": 11}, \"created_at\": 11}",
  eredis:q(RedisClient, ["SET","last_value_variables_json:" ++ VariableId, ValueData]),
  initialize_variables(RedisClient, OwnerId, DeviceLabel, Rest),
  ok.

initialize_devices(_, _, _, []) ->
  ok;
initialize_devices(RedisClient, OwnerId, Token, [DeviceLabel, DeviceId, Variables | Rest]) ->
  eredis:q(RedisClient, ["HSET", "reactor_device_data/" ++ DeviceId, "/device_label", DeviceLabel]),
  eredis:q(RedisClient, ["HSET", "reactor_variables/" ++ OwnerId, "/" ++ DeviceLabel, DeviceId]),
  eredis:q(RedisClient, ["SADD", "reactor_devices_with_permissions/view_value/" ++ Token, DeviceId]),
  initialize_variables(RedisClient, OwnerId, DeviceLabel, Variables),
  initialize_devices(RedisClient, OwnerId, Token, Rest),
  ok.

initialize_mqtt_cache(RedisClient, Token, OwnerId, Devices) ->
  eredis:q(RedisClient, ["HSET", "reactor_tokens/" ++ Token, "/owner_id", OwnerId]),
  eredis:q(RedisClient, ["HSET", "reactor_tokens/" ++ Token, "/permissions_type", "device"]),
  initialize_devices(RedisClient, OwnerId, Token, Devices),
  ok.

retainer_test_() ->
  Devices = ["d1", "d1_id", ["v1", "v1_d1_id", "v2", "v2_d1_id", "v3", "v3_d1_id"],
    "d2", "d2_id", ["v1", "v1_d2_id", "v2", "v2_d2_id", "v3", "v3_d2_id"]],
  {ok, RedisClient} = eredis:start_link(),
  eredis:q(RedisClient, ["FLUSHDB"]),
  initialize_mqtt_cache(RedisClient, "token", "owner_id", Devices),
  [Topic, Value | _Rest] = get_values_from_topic("/v1.6/users/token/devices/d1/v1"),
  [
    ?_assertEqual(<<"/v1.6/devices/d1/v1">>, Topic),
    ?_assertEqual(
      <<"{\"value\": 11.1, \"timestamp\": 11, \"context\": {\"a\": 11}, \"created_at\": 11}">>, Value)
  ].

retainer_empty_test_() ->
  {ok, RedisClient} = eredis:start_link(),
  eredis:q(RedisClient, ["FLUSHDB"]),
  Result = get_values_from_topic("/v1.6/users/token/devices/d1/v1"),
  [
    ?_assertEqual(Result, [])
  ].