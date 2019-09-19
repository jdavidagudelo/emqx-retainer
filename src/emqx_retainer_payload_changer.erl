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
  Password = maps:get(reactor_cache_password, Options, ok),
  {ok, RedisClient} = eredis:start_link(Host, Port, Database, Password, no_reconnect),
  RedisClient.

get_ubidots_redis_client(Options) ->
  Host = maps:get(ubidots_cache_host_name, Options, "127.0.0.1"),
  Port = maps:get(ubidots_cache_port, Options, 6379),
  Database = maps:get(ubidots_cache_database, Options, 1),
  Password = maps:get(ubidots_cache_password, Options, ok),
  {ok, RedisClient} = eredis:start_link(Host, Port, Database, Password, no_reconnect),
  RedisClient.

get_data_from_topic(Topic) ->
  Split = string:split(Topic, "/", all),
  SplitArray = array:from_list(Split),
  Token = array:get(3, SplitArray),
  DeviceLabel = array:get(5, SplitArray),
  VariableLabel = array:get(6, SplitArray),
  {Token, DeviceLabel, VariableLabel}.


is_valid_subscribe_topic(Topic) ->
  Split = string:split(Topic, "/", all),
  SplitArray = array:from_list(Split),
  array:size(SplitArray) >= 8.



