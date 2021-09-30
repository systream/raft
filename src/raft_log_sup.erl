%%%-------------------------------------------------------------------
%% @doc raft collaborator sup
%% @end
%%%-------------------------------------------------------------------

-module(raft_log_sup).

-behaviour(supervisor).

-export([start_link/0, start_worker/1, stop_worker/1]).

-export([init/1]).

-define(SERVER, ?MODULE).

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

-spec start_worker(proplists:proplist()) ->
  {ok, pid()} | {error, term()}.
start_worker(Opts) ->
  supervisor:start_child(?MODULE, [Opts]).

-spec stop_worker(pid()) -> ok | {error, term()}.
stop_worker(Pid) ->
  supervisor:terminate_child(?MODULE, Pid).

init([]) ->
    SupFlags = #{strategy => simple_one_for_one,
                 intensity => 10,
                 period => 100},
    ChildSpecs = [#{id => raft_log_worker,
                    start => {raft_log_worker, start_link, []},
                    restart => temporary,
                    shutdown => 5000,
                    type => worker,
                    modules => [raft_log_worker]}
                 ],
    {ok, {SupFlags, ChildSpecs}}.

%% internal functions
