%%%-------------------------------------------------------------------
%%% @author jdavidagudelo
%%% @copyright (C) 2019, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 06. Aug 2019 4:55 p. m.
%%%-------------------------------------------------------------------
-module(emqx_topic_changer).
-author("jdavidagudelo").

-include("emqx.hrl").


%% API
-export([set_topic/2]).

-spec(set_topic(emqx:topic(), emqx_types:message()) -> emqx_types:message()).
set_topic(Topic, Msg) ->
    NewTopic = re:replace(Topic, "/users/[^/]+","", [{return,list}]),
    Msg#message{topic = NewTopic}.
