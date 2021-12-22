%%%-------------------------------------------------------------------
%%% @author Peter Tihanyi
%%% @copyright (C) 2021, systream
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(raft_log_worker).

-behaviour(gen_server).

-export([append/3, stream/3, stream_log/4, read_stream/2]).
-export([start_link/1,
         init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(SERVER, ?MODULE).

%-define(LOG(Msg, Args), io:format(user, "[~p] " ++ Msg, [erlang:system_time(millisecond) | Args])).
-define(LOG(_Msg, A_rgs), ok).

-record(retention, {
  time = 604800, % one week, in sec
  max_item = 1000000
}).

-type retention() :: #retention{}.
-type log_pos() :: non_neg_integer().

-record(state, {
  retention = #retention{} :: retention(),
  first = undefined :: log_pos() | undefined,
  last = undefined :: log_pos() | undefined,
  ref :: term()
}).

-record(append_req, {
  last_pos :: pos_integer(),
  data :: term()
}).

-record(stream_req, {
  pos :: pos_integer() | first,
  target :: pid()
}).

-type(stream_ref() :: pid()).

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================

-spec start_link(proplists:proplist()) -> {ok, pid()}.
start_link(Opts) ->
  gen_server:start_link(?MODULE, Opts, []).

-spec append(pid(), pos_integer(), term()) -> {ok, pos_integer()}.
append(Pid, Pos, Data) ->
  gen_server:call(Pid, #append_req{last_pos = Pos, data = Data}).

-spec stream(pid(), pid(), non_neg_integer() | first) ->
  {ok, stream_ref()} | {error, {bad_log_pos, RequestedLog, {FirstLog, LastLog}}} when
  RequestedLog :: log_pos(),
  FirstLog :: log_pos(),
  LastLog :: log_pos().
stream(Pid, TargetPid, LogPos) ->
  gen_server:call(Pid, #stream_req{pos = LogPos, target = TargetPid}).

-spec read_stream(stream_ref(), non_neg_integer() | infinity) ->
  {ok, non_neg_integer(), term()} | '$end_of_stream' | timeout.
read_stream({Pid, _} = StreamRef, Timeout) ->
  Ref = erlang:monitor(process, Pid),
  receive
    {log_stream, StreamRef, Pos, Data} ->
      demonitor(Ref, [flush]),
      {ok, Pos, Data};
    {log_stream_end, StreamRef} ->
      demonitor(Ref, [flush]),
      '$end_of_stream';
    {'DOWN', Ref, process, StreamRef, _Reason} ->
      '$end_of_stream'
  after Timeout ->
    demonitor(Ref, [flush]),
    timeout
  end.

init(Opts) ->
  erlang:process_flag(trap_exit, true),
  State = #state{ref = ets:new(raft_log, [protected, ordered_set])},
  NewState = opts(Opts, State),
  erlang:send_after(1000, self(), cleanup),
  {ok, NewState}.

opts([], #state{} = State) ->
  State;
opts([{retention_time, Time} | Opts], #state{retention = Retention} = State) ->
  opts(Opts, State#state{retention = Retention#retention{time = Time}}).

handle_call(#append_req{data = Data, last_pos = Pos}, _From,
            #state{ref = Ref, last = undefined, first = undefined} = State) ->
  ?LOG("[~p] Lp: ~p - ~p <> ~p ~n", [Ref, Pos, undefined, undefined]),
  NewPos = Pos + 1,
  insert_log(Ref, NewPos, Data),
  {reply, {ok, NewPos}, State#state{last = NewPos, first = NewPos}};
handle_call(#append_req{data = Data, last_pos = Pos}, _From,
            #state{ref = Ref, first = First, last = Last} = State) when Last =:= Pos ->
  ?LOG("[~p] Lp: ~p - F: ~p <>L:~p ~n", [Ref, Pos, First, Last]),
  NewPos = Last+1,
  insert_log(Ref, NewPos, Data),
  {reply, {ok, NewPos}, State#state{last = NewPos}};
handle_call(#append_req{last_pos = Pos} = Req, From,
            #state{ref = Ref, first = First, last = Last} = State) when Last > Pos ->
  ?LOG("[~p] LastPos > Cpos Lp: ~p - F: ~p <>L:~p ~n", [Ref, Pos, First, Last]),
  NewPos = Pos+1,
  [ets:delete(Ref, PosToDel) || PosToDel <- lists:seq(NewPos, Last)],
  NewState = case NewPos of
                1 -> State#state{last = undefined, first = undefined};
                _ when First > Pos -> State#state{last = undefined, first = undefined};
                _  -> State#state{last = Pos}
              end,
  handle_call(Req, From, NewState);

% nothing to stream, send end steam msg
handle_call(#stream_req{target = Target}, _From, State = #state{first = undefined}) ->
  StreamRef = {self(), make_ref()},
  ?LOG("Nothing to streamlog ~n", []),
  send_log_stream_end(Target, StreamRef),
  {reply, {ok, StreamRef}, State};
handle_call(#stream_req{pos = first, target = Target}, _From,
            State = #state{ref = Ref, first = First}) ->
  ?LOG("Streamlog Set to first: ~p ~n", [First]),
  StreamRef = {self(), make_ref()},
  ?LOG("streamlog to first ~p ~n", [First]),
  {ok, _} = proc_lib:start_link(?MODULE, stream_log, [Ref, StreamRef, Target, First]),
  {reply, {ok, StreamRef}, State};
% requested stream from the last log pos, nothing to stream
handle_call(#stream_req{pos = Pos, target = Target}, _From, State = #state{last = Last})
  when Pos =:= Last ->
  StreamRef = {self(), make_ref()},
  ?LOG("Nothing to streamlog lastpos == last ~n", []),
  send_log_stream_end(Target, StreamRef),
  {reply, {ok, StreamRef}, State};
handle_call(#stream_req{pos = LogPos} = Stream, From, State = #state{first = First})
  when LogPos+1 < First ->
  handle_call(Stream#stream_req{pos = first}, From, State);
% stream request is out of range
handle_call(#stream_req{pos = LogPos}, _From, State = #state{first = First, last = Last})
  when LogPos+1 > Last ->
  {reply, {error, {bad_log_pos, LogPos, {First, Last}}}, State};
handle_call(#stream_req{pos = Pos, target = Target}, _From, State = #state{ref = Ref})
  when is_number(Pos) ->
  StreamRef = {self(), make_ref()},
  ?LOG("streamlog to ~p ~n", [Pos]),
  {ok, _Pid} = proc_lib:start_link(?MODULE, stream_log, [Ref, StreamRef, Target, Pos+1]),
  ?LOG("streamlog process ~p ~n", [_Pid]),
  {reply, {ok, StreamRef}, State};

handle_call(_Request, _From, State = #state{}) ->
  ?LOG("unhandled_request: ~p~n State: ~p~n", [_Request, State]),
  {reply, ok, State}.

handle_cast(_Request, State = #state{}) ->
  {noreply, State}.

% log_streamer process died, end of streaming
handle_info({'EXIT', _Pid, _Reason}, State = #state{}) ->
  ?LOG("Pid stopped: ~p -> ~p~n", [_Pid, _Reason]),
  %send_log_stream_end(Target, StreamRef),
  {noreply, State};
handle_info(cleanup, State = #state{first = undefined}) ->
  erlang:send_after(1000, self(), cleanup),
  {noreply, State};
handle_info(cleanup, State = #state{}) ->
  erlang:send_after(1000, self(), cleanup),
  NewState = cleanup(State, 100),
  {noreply, NewState}.

terminate(_Reason, _State = #state{}) ->
  ok.

stream_log(Ref, StreamRef, Target, LogPos) ->
  % Stream the first item then ack back
  ?LOG("[~p] stream: ~p~n", [StreamRef, LogPos]),
  send_log_stream(Target, StreamRef, LogPos, get_log(Ref, LogPos)),
  proc_lib:init_ack({ok, self()}),
  do_stream_log(Ref, StreamRef, Target, ets:next(Ref, LogPos)).

%%%===================================================================
%%% Internal functions
%%%===================================================================

do_stream_log(_Ref, StreamRef, Target, '$end_of_table') ->
  send_log_stream_end(Target, StreamRef);
do_stream_log(Ref, StreamRef, Target, LogPos) ->
  send_log_stream(Target, StreamRef, LogPos, get_log(Ref, LogPos)),
  do_stream_log(Ref, StreamRef, Target, ets:next(Ref, LogPos)).

send_log_stream(TargetPid, StreamRef, Pos, Data) ->
  TargetPid ! {log_stream, StreamRef, Pos, Data}.

send_log_stream_end(TargetPid, StreamRef) ->
  TargetPid ! {log_stream_end, StreamRef}.

get_log(Ref, LogPos) ->
  [{_Pos, Data, _Ts}] = ets:lookup(Ref, LogPos),
  Data.

get_log_ts(Ref, LogPos) ->
  [{_Pos, _Data, Ts}] = ets:lookup(Ref, LogPos),
  Ts.

insert_log(Ref, LogPos, Data) ->
  true = ets:insert(Ref, {LogPos, Data, get_ts()}).

get_ts() ->
  os:system_time(second).

cleanup(State, 0) ->
  State;
cleanup(#state{ref = Ref, first = First, retention = #retention{time = RetentionTime}} = State,
        Tokens) ->
  %io:format(user, "[~p] cleanup ~p -> rem token: ~p~n", [Ref, First, Tokens]),
  case get_log_ts(Ref, First)+RetentionTime < get_ts() of
    true ->
      ets:delete(Ref, First),
      case State#state.last == First of
        true ->
          State#state{first = undefined, last = undefined};
        _ ->
          cleanup(State#state{first = First+1}, Tokens-1)
      end;
    _ ->
      State
  end.