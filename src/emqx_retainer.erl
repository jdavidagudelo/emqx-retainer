%% Copyright (c) 2013-2019 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.

-module(emqx_retainer).

-behaviour(gen_server).

-include("emqx_retainer.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("stdlib/include/ms_transform.hrl").

-export([start_link/1]).

-export([ load/1
        , unload/0
        ]).

-export([ on_session_subscribed/3
        ]).

-export([clean/1]).

%% gen_server callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

-record(state, {stats_fun, stats_timer, expiry_timer}).

%%------------------------------------------------------------------------------
%% Load/Unload
%%------------------------------------------------------------------------------

load(_Env) ->
    emqx:hook('session.subscribed', fun ?MODULE:on_session_subscribed/3, []).

on_session_subscribed(#{client_id := _ClientId}, Topic, _) ->
  dispatch_ubidots_messages(Topic).

unload() ->
    emqx:unhook('session.subscribed', fun ?MODULE:on_session_subscribed/3).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

%% @doc Start the retainer
-spec(start_link(Env :: list()) -> emqx_types:startlink_ret()).
start_link(Env) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [Env], []).

clean(Topic) when is_binary(Topic) ->
    case emqx_topic:wildcard(Topic) of
        true -> match_delete_messages(Topic);
        false ->
            Fun = fun() ->
                      case mnesia:read({?TAB, Topic}) of
                          [] -> 0;
                          [_M] -> mnesia:delete({?TAB, Topic}), 1
                      end
                  end,
            {atomic, N} = mnesia:transaction(Fun), N
    end.

%%------------------------------------------------------------------------------
%% gen_server callbacks
%%------------------------------------------------------------------------------

init([Env]) ->
    ok.

handle_call(Req, _From, State) ->
    ?LOG(error, "[Retainer] Unexpected call: ~p", [Req]),
    {reply, ignored, State}.

handle_cast(Msg, State) ->
    ?LOG(error, "[Retainer] Unexpected cast: ~p", [Msg]),
    {noreply, State}.

handle_info(stats, State = #state{stats_fun = StatsFun}) ->
    StatsFun(retained_count()),
    {noreply, State, hibernate};

handle_info(expire, State) ->
    expire_messages(),
    {noreply, State, hibernate};

handle_info(Info, State) ->
    ?LOG(error, "[Retainer] Unexpected info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, _State = #state{stats_timer = TRef1, expiry_timer = TRef2}) ->
    timer:cancel(TRef1), timer:cancel(TRef2).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


dispatch_ubidots_message([]) ->
  ok;
dispatch_ubidots_message([Msg = #message{topic = Topic} | Rest]) ->
  self() ! {dispatch, Topic, [Msg]},
  dispatch_ubidots_message(Rest).

dispatch_ubidots_messages(Topic) ->
  NewMessages = emqx_retainer_payload_changer:get_retained_messages_from_topic(Topic),
  dispatch_ubidots_message(NewMessages).

-spec(match_delete_messages(binary()) -> integer()).
match_delete_messages(Filter) ->
    %% TODO: optimize later...
    Fun = fun(#retained{topic = Name}, Topics) ->
              case emqx_topic:match(Name, Filter) of
                true -> mnesia:delete({?TAB, Name}), [Name | Topics];
                false -> Topics
              end
          end,
    Topics = mnesia:async_dirty(fun mnesia:foldl/3, [Fun, [], ?TAB]),
    mnesia:transaction(
        fun() ->
            lists:foreach(fun(Topic) -> mnesia:delete({?TAB, Topic}) end, Topics)
        end),
    length(Topics).

-spec(expire_messages() -> any()).
expire_messages() ->
    NowMs = emqx_time:now_ms(),
    mnesia:transaction(
        fun() ->
            Match = ets:fun2ms(
                        fun(#retained{topic = Topic, expiry_time = ExpiryTime})
                            when ExpiryTime =/= 0 andalso NowMs > ExpiryTime -> Topic
                        end),
            Topics = mnesia:select(?TAB, Match, write),
            lists:foreach(fun(Topic) -> mnesia:delete({?TAB, Topic})
                           end, Topics)
        end).

-spec(retained_count() -> non_neg_integer()).
retained_count() -> mnesia:table_info(?TAB, size).

