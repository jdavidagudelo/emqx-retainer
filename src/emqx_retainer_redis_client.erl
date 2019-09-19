%%%-------------------------------------------------------------------
%%% @author jdavidagudelo
%%% @copyright (C) 2019, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 19. Sep 2019 11:49 a. m.
%%%-------------------------------------------------------------------
-module(emqx_retainer_redis_client).
-author("jdavidagudelo").

%% API
-export([]).


get_last_value_variable(Topic, Msg) ->
    NewTopic = re:replace(Topic, "/users/[^/]+","", [{return,list}]),
    Msg#message{topic = NewTopic}.

