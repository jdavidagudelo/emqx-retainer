%%%-------------------------------------------------------------------
%%% @author jdavidagudelo
%%% @copyright (C) 2019, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 06. Aug 2019 4:55 p. m.
%%%-------------------------------------------------------------------
-module(emqx_retainer_topic_changer).
-author("jdavidagudelo").

-include("emqx_retainer.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("stdlib/include/ms_transform.hrl").


%% API
-export([set_topic/2]).

-spec(set_topic(emqx:topic(), emqx_types:message()) -> emqx_types:message()).
set_topic(Topic, Msg) ->
    NewTopic = re:replace(Topic, "/users/[^/]+","", [{return,list}]),
    Msg#message{topic = NewTopic, payload = "THIS IS A PAYLOAD"}.
