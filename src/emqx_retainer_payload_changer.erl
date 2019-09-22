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
  eredis:stop(ReactorRedisClient),
  eredis:stop(UbidotsRedisClient),
  Values.

get_messages([]) ->
  [];
get_messages([Topic, Value|Rest]) ->
  NewMessage = emqx_message:make(Topic, Value),
  [NewMessage | get_messages(Rest)].

get_retained_messages_from_topic(Topic) ->
  Values = get_values_from_topic(Topic),
  get_messages(Values).
