%%%-------------------------------------------------------------------
%% @doc raft collaborator sup
%% @end
%%%-------------------------------------------------------------------

-module(raft_server_sup).

-behaviour(supervisor).

-export([start_link/0, start_server/1]).

-export([init/1]).

-define(SERVER, ?MODULE).

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

-spec start_server(CallbackModule :: module()) ->
  {ok, pid()} | {error, term()}.
start_server(CallbackModule) ->
  supervisor:start_child(?MODULE, [CallbackModule]).

init([]) ->
    SupFlags = #{strategy => simple_one_for_one,
                 intensity => 10,
                 period => 100},
    ChildSpecs = [#{id => raft_server,
                    start => {raft_server, start_link, []},
                    restart => temporary,
                    shutdown => 15000,
                    type => worker,
                    modules => [raft_server]}
                 ],
    {ok, {SupFlags, ChildSpecs}}.

%% internal functions
