-module(node4).

-export([start/1, start/2]).

-define(Timeout, 10000).
-define(Stabilize, 1000).

start(Id) ->
    start(Id, nil).

start(Id, Peer) ->
    timer:start(),
    spawn(fun() -> init(Id, Peer) end).

init(Id, Peer) ->
    Predecessor = nil,
    Next = nil,
    {ok, {Skey, Spid}} = connect(Id, Peer),
    Sref = monitor(Spid),
    schedule_stabilize(),
    node(Id, Predecessor, {Skey, Sref, Spid}, Next, storage:create(),storage:create()).

connect(Id, nil) ->
    {ok, {Id, self()}};

connect(_Id, Peer) ->
    Qref = make_ref(),
    Peer ! {key, Qref, self()},
    receive
        {Qref, Skey} ->
            {ok, {Skey, Peer}}
    after ?Timeout ->
        io:format("Time out: no response~n", []),
        {error, timeout}
    end.

node(Id, Predecessor, Successor, Next, Store,Replica) ->
    receive
        {key, Qref, Peer} ->
            Peer ! {Qref, Id},
            node(Id, Predecessor, Successor, Next, Store,Replica);

        {notify, New} ->
            {Pred, Keep,NewReplica} = notify(New, Id, Predecessor, Successor,Store,Replica),
            node(Id, Pred, Successor, Next, Keep,NewReplica);

        {request, Peer} ->
            request(Peer, Predecessor, Successor),
            node(Id, Predecessor, Successor, Next, Store,Replica);

        {status, Pred, Nx} ->
            {Succ, Nxt} = stabilize(Pred, Nx, Id, Successor),
            node(Id, Predecessor, Succ, Nxt, Store,Replica);

        stabilize ->
            stabilize(Successor),
            node(Id, Predecessor, Successor, Next, Store,Replica);

        probe ->
            create_probe(Id, Successor),
            node(Id, Predecessor, Successor, Next, Store,Replica);

        {probe, Id, Nodes, T} ->
            remove_probe(T, Nodes),
            node(Id, Predecessor, Successor, Next, Store,Replica);

        {probe, Ref, Nodes, T} ->
            forward_probe(Ref, T, Nodes, Id, Successor),
            node(Id, Predecessor, Successor, Next, Store,Replica);
        {add, Key, Value, Qref, Client} ->
            Added = add(Key, Value, Qref, Client, Id, Predecessor, Successor, Store),
            node(Id, Predecessor, Successor, Next, Added, Replica);

        {replicate, Key, Value} ->
            NewReplica = storage:add(Key, Value, Replica),
            node(Id, Predecessor, Successor, Next, Store, NewReplica);
        {cloneReplica, AnotherReplica} ->
            node(Id, Predecessor, Successor, Next, Store, AnotherReplica);
        {lookup, Key, Qref, Client} ->
            lookup(Key, Qref, Client, Id, Predecessor, Successor, Store),
            node(Id, Predecessor, Successor, Next, Store,Replica);

        {handover, Elements} ->
            Merged = storage:merge(Elements, Store),
            node(Id, Predecessor, Successor, Next, Merged,Replica);

        %% A monitored predecessor or successor died
        {'DOWN', Ref, process, _, _} ->
            {Pred, Succ, Nxt,NewStore,NewReplica} = down(Ref, Predecessor, Successor, Next,Store,Replica),
            node(Id, Pred, Succ, Nxt, NewStore,NewReplica)
    end.

%% Ask successor for predecessor + successor
stabilize({_, _, Spid}) ->
    Spid ! {request, self()}.

stabilize(Pred, Nx, Id, Successor) ->
    {Skey, Sref, Spid} = Successor,
    case Pred of
        nil ->
            Spid ! {notify, {Id, self()}},
            {Successor, Nx};

        {Id, _} ->
            {Successor, Nx};

        {Skey, _} ->
            Spid ! {notify, {Id, self()}},
            {Successor, Nx};

        {Xkey, Xpid} ->
            case key:between(Xkey, Id, Skey) of
                true ->
                    %% X becomes our new successor
                    drop(Sref),
                    Xref = monitor(Xpid),
                    NewSuccessor = {Xkey, Xref, Xpid},
                    stabilize(NewSuccessor),
                    {NewSuccessor, {Skey, Spid}};

                false ->
                    Spid ! {notify, {Id, self()}},
                    {Successor, Nx}
            end
    end.

schedule_stabilize() ->
    timer:send_interval(?Stabilize, self(), stabilize).

%% Reply with predecessor and our successor (without monitor refs)
request(Peer, Predecessor, {Skey, _, Spid}) ->
    case Predecessor of
        nil ->
            Peer ! {status, nil, {Skey, Spid}};
        {Pkey, _, Ppid} ->
            Peer ! {status, {Pkey, Ppid}, {Skey, Spid}}
    end.

%% Accept a new predecessor and monitor it
notify({Nkey, Npid}, Id, Predecessor, {_, _, Spid}, Store, Replica) ->
    case Predecessor of
        nil ->
            Nref = monitor(Npid),
            {Keep, NewReplica} = handover(Id, Store, Nkey, Npid),

            Npid ! {cloneReplica, Replica},

            Spid ! {cloneReplica, Keep},

            {{Nkey, Nref, Npid}, Keep, NewReplica};

        {Pkey, Pref, _} ->
            case key:between(Nkey, Pkey, Id) of
                true ->
                    drop(Pref),
                    Nref = monitor(Npid),
                    {Keep, NewReplica} = handover(Id, Store, Nkey, Npid),

                    Npid ! {cloneReplica, Replica},
                    Spid ! {cloneReplica, Keep},

                    {{Nkey, Nref, Npid}, Keep, NewReplica};

                false ->
                    {Predecessor, Store, Replica}
            end
    end.

handover(Id, Store, Nkey, Npid) ->
    {Rest, Keep} = storage:split(Id, Nkey, Store),
    Npid ! {handover, Rest},
    {Keep, Rest}.

%% Monitor helpers
monitor(Pid) ->
    erlang:monitor(process, Pid).

drop(nil) ->
    ok;
drop(Ref) ->
    erlang:demonitor(Ref, [flush]).

%% Predecessor died: simply forget it
down(Ref, {_, Ref, _}, {Skey, Sref, Spid}, Next, Store, Replica) ->
    NewStore = storage:merge(Store, Replica),
    Spid ! {cloneReplica, NewStore},
    {nil, {Skey, Sref, Spid}, Next, NewStore, storage:create()};

%% Successor died: promote Next to successor
down(Ref, Predecessor, {_, Ref, _}, {Nkey, Npid},Store,Replica) ->
    Nref = monitor(Npid),
    NewSuccessor = {Nkey, Nref, Npid},


    Npid ! {cloneReplica, Store},

    stabilize(NewSuccessor),
    {Predecessor, NewSuccessor, nil,Store,Replica};

%% Ignore DOWN messages that do not belong to current neighbours
down(_, Predecessor, Successor, Next,Store,Replica) ->
    {Predecessor, Successor, Next,Store,Replica}.

create_probe(Id, {_, _, Spid}) ->
    T = erlang:system_time(microsecond),
    Spid ! {probe, Id, [Id], T}.

forward_probe(Ref, T, Nodes, Id, {_, _, Spid}) ->
    Spid ! {probe, Ref, [Id | Nodes], T}.

remove_probe(T, Nodes) ->
    Time = erlang:system_time(microsecond) - T,
    io:format("Probe completed in ~p us~n", [Time]),
    io:format("Nodes: ~p~n", [lists:reverse(Nodes)]).

add(Key, Value, Qref, Client, Id, {Pkey, _, _}, {_, _, Spid}, Store) ->
    case key:between(Key, Pkey, Id) of
        true ->
            NewStore = storage:add(Key, Value, Store),
            Spid ! {replicate, Key, Value},
            Client ! {Qref, ok},
            NewStore;
        false ->
            Spid ! {add, Key, Value, Qref, Client},
            Store
    end.



lookup(Key, Qref, Client, Id, {Pkey, _, _}, {_, _, Spid}, Store) ->
    case key:between(Key, Pkey, Id) of
        true ->
            Result = storage:lookup(Key, Store),
            Client ! {Qref, Result};
        false ->
            Spid ! {lookup, Key, Qref, Client}
    end.