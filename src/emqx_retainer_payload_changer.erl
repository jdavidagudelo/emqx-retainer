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
-include("emqx_retainer.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("stdlib/include/ms_transform.hrl").


%% API
-export([]).

get_retainer_configuration() ->
  FilePathConfiguration = filename:join(file:get_cwd(), "retainer_changer", "retainer_changer.conf"),
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

get_values_variables(RedisClient, ScriptData, Topic, VariablesData) ->
  VariablesDataArray = array:from_list(VariablesData),
  Args = ["EVAL", ScriptData, array:size(VariablesDataArray)] ++ VariablesData,
  {ok, Result} = eredis:q(RedisClient, Args),
  Result.

