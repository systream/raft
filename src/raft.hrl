%%%-------------------------------------------------------------------
%%% @author Peter Tihanyi
%%% @copyright (C) 2021, systream
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------

-define(LOG(Msg, Args), io:format(user, "[~p] " ++ Msg, [erlang:system_time(millisecond) | Args])).
%-define(LOG(_Msg, _Args), ok).

-type(raft_term() :: non_neg_integer()).
-type(command() :: term()).
-type(log_index() :: pos_integer()).

