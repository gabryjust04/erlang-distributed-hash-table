-module(node3).

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
    node(Id, Predecessor, {Skey, Sref, Spid}, Next, storage:create()).

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

node(Id, Predecessor, Successor, Next, Store) ->
    receive
        {key, Qref, Peer} ->
            Peer ! {Qref, Id},
            node(Id, Predecessor, Successor, Next, Store);

        {notify, New} ->
            {Pred, Keep} = notify(New, Id, Predecessor, Store),
            node(Id, Pred, Successor, Next, Keep);

        {request, Peer} ->
            request(Peer, Predecessor, Successor),
            node(Id, Predecessor, Successor, Next, Store);

        {status, Pred, Nx} ->
            {Succ, Nxt} = stabilize(Pred, Nx, Id, Successor),
            node(Id, Predecessor, Succ, Nxt, Store);

        stabilize ->
            stabilize(Successor),
            node(Id, Predecessor, Successor, Next, Store);

        probe ->
            create_probe(Id, Successor),
            node(Id, Predecessor, Successor, Next, Store);

        {probe, Id, Nodes, T} ->
            remove_probe(T, Nodes),
            node(Id, Predecessor, Successor, Next, Store);

        {probe, Ref, Nodes, T} ->
            forward_probe(Ref, T, Nodes, Id, Successor),
            node(Id, Predecessor, Successor, Next, Store);

        {add, Key, Value, Qref, Client} ->
            Added = add(Key, Value, Qref, Client, Id, Predecessor, Successor, Store),
            node(Id, Predecessor, Successor, Next, Added);

        {lookup, Key, Qref, Client} ->
            lookup(Key, Qref, Client, Id, Predecessor, Successor, Store),
            node(Id, Predecessor, Successor, Next, Store);

        {handover, Elements} ->
            Merged = storage:merge(Elements, Store),
            node(Id, Predecessor, Successor, Next, Merged);

        %% A monitored predecessor or successor died
        {'DOWN', Ref, process, _, _} ->
            {Pred, Succ, Nxt} = down(Ref, Predecessor, Successor, Next),
            node(Id, Pred, Succ, Nxt, Store)
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
notify({Nkey, Npid}, Id, Predecessor, Store) ->
    case Predecessor of
        nil ->
            Nref = monitor(Npid),
            Keep = handover(Id, Store, Nkey, Npid),
            {{Nkey, Nref, Npid}, Keep};

        {Pkey, Pref, _} ->
            case key:between(Nkey, Pkey, Id) of
                true ->
                    drop(Pref),
                    Nref = monitor(Npid),
                    Keep = handover(Id, Store, Nkey, Npid),
                    {{Nkey, Nref, Npid}, Keep};

                false ->
                    {Predecessor, Store}
            end
    end.

handover(Id, Store, Nkey, Npid) ->
    {Rest, Keep} = storage:split(Id, Nkey, Store),
    Npid ! {handover, Rest},
    Keep.

%% Monitor helpers
monitor(Pid) ->
    erlang:monitor(process, Pid).

drop(nil) ->
    ok;
drop(Ref) ->
    erlang:demonitor(Ref, [flush]).

%% Predecessor died: simply forget it
down(Ref, {_, Ref, _}, Successor, Next) ->
    {nil, Successor, Next};

%% Successor died: promote Next to successor
down(Ref, Predecessor, {_, Ref, _}, {Nkey, Npid}) ->
    Nref = monitor(Npid),
    NewSuccessor = {Nkey, Nref, Npid},
    stabilize(NewSuccessor),
    {Predecessor, NewSuccessor, nil};

%% Ignore DOWN messages that do not belong to current neighbours
down(_, Predecessor, Successor, Next) ->
    {Predecessor, Successor, Next}.

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
            Client ! {Qref, ok},
            storage:add(Key, Value, Store);
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