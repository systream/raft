%%%-------------------------------------------------------------------
%% @doc raft collaborator sup
%% @end
%%%-------------------------------------------------------------------

-module(raft_collaborator_sup).

-behaviour(supervisor).

-export([start_link/0, start_collaborator/1]).

-export([init/1]).

-define(SERVER, ?MODULE).

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

-spec start_collaborator(CallbackModule :: module()) ->
  {ok, pid()} | {error, term()}.
start_collaborator(CallbackModule) ->
  supervisor:start_child(?MODULE, [CallbackModule]).

init([]) ->
    SupFlags = #{strategy => simple_one_for_one,
                 intensity => 10,
                 period => 100},
    ChildSpecs = [#{id => raft_collaborator,
                    start => {raft_collaborator, start_link, []},
                    restart => temporary,
                    shutdown => 5000,
                    type => worker,
                    modules => [raft_collaborator]}
                 ],
    {ok, {SupFlags, ChildSpecs}}.

%% internal functions
