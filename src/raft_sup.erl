%%%-------------------------------------------------------------------
%% @doc raft top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(raft_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

-define(SERVER, ?MODULE).

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

init([]) ->
    SupFlags = #{strategy => one_for_one,
                 intensity => 10,
                 period => 100},
    CollaboratorSup = #{id => raft_collaborator_sup,
                        start => {raft_collaborator_sup, start_link, []},
                        restart => permanent,
                        shutdown => 5000,
                        type => supervisor,
                        modules => [raft_collaborator_sup, raft_collaborator]},
    Log = #{id => raft_log_sup,
                        start => {raft_log_sup, start_link, []},
                        restart => permanent,
                        shutdown => 5000,
                        type => worker,
                        modules => [raft_log_sup]},
    {ok, {SupFlags, [Log, CollaboratorSup]}}.
