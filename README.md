# Raft protocol

Implementation of [replicated state machine (raft)](https://raft.github.io/) protocol.

Implemented:
* Leader election
* Log replication
* Cluster changes
* Log compaction (snapshotting)
* Request repetition filtering (bloom)

## Usage

Before starting a new raft server, you need to implement
`raft_server` behavior. 

 * `init/0`: return with the new staring state
 * `handle_command/2`: execute commands that can modify state
 * `handle_query/2`: execute queries which cannot change state

### Start new raft server
```erlang
{ok, Pid} = raft:start(callback_module).
```

### Commands
```erlang
raft:command(Pid, {do_stuff, Stuff}).
```

### Query
```erlang
raft:query(Pid, {get_stuff, Stuff}).
```

## Cluster changes

### Join new member to the cluster
```erlang
raft:join(PidA, PidB).
```

### Leave member
```erlang
raft:leave(PidA, PidB).
```

### Get status info

```erlang
#{role => StateName,
  term => CurrentTerm,
  leader => LeaderPid,
  cluster_members => LisfOfPids,
  log_last_index => LastIndex,
  committed_index => Ci,
  last_applied => La,
  log => Last10LogEntry
} = raft:status(Pid)
```


Build
-----

***WARNING***: Not yet prod ready

    $ rebar3 compile
