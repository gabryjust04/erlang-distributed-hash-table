-module(node2).

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
    {ok, Successor} = connect(Id, Peer),
    schedule_stabilize(),
    node(Id, Predecessor, Successor,storage:create()).

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

node(Id, Predecessor, Successor,Store) ->
    receive
        {key, Qref, Peer} ->
            Peer ! {Qref, Id},
            node(Id, Predecessor, Successor,Store);

        {notify, New} ->
            {Pred,Keep} = notify(New, Id, Predecessor,Store),
            node(Id, Pred, Successor,Keep);

        {request, Peer} ->
            request(Peer, Predecessor),
            node(Id, Predecessor, Successor,Store);

        {status, Pred} ->
            Succ = stabilize(Pred, Id, Successor),
            node(Id, Predecessor, Succ,Store);

        stabilize ->
            stabilize(Successor),
            node(Id, Predecessor, Successor,Store);
        probe ->
            create_probe(Id, Successor),
            node(Id, Predecessor, Successor,Store);

        {probe, Id, Nodes, T} ->
            remove_probe(T, Nodes),
            node(Id, Predecessor, Successor,Store);

        {probe, Ref, Nodes, T} ->
            forward_probe(Ref, T, Nodes, Id, Successor),
            node(Id, Predecessor, Successor,Store);
        {add,Key,Value,Qref,Client} ->
            Added = add(Key,Value,Qref,Client,Id,Predecessor,Successor,Store),
            node(Id,Predecessor,Successor,Added);
        {lookup,Key,Qref,Client} ->
            lookup(Key,Qref,Client,Id,Predecessor,Successor,Store),
            node(Id,Predecessor,Successor,Store);
        {handover,Elements} -> 
            Merged = storage:merge(Elements, Store),
            node(Id,Predecessor,Successor,Merged)
    end.

%% Ask the successor for its predecessor
stabilize({_, Spid}) ->
    Spid ! {request, self()}.

stabilize(Pred, Id, Successor) ->
    {Skey, Spid} = Successor,
    case Pred of
        nil ->
            Spid ! {notify, {Id, self()}},
            Successor;

        {Id, _} ->
            Successor;

        {Skey, _} ->
            Spid ! {notify, {Id, self()}},
            Successor;

        {Xkey, Xpid} ->
            case key:between(Xkey, Id, Skey) of
                true ->
                    %% X is a better successor
                    stabilize({Xkey, Xpid}),
                    {Xkey, Xpid};
                false ->
                    Spid ! {notify, {Id, self()}},
                    Successor
            end
    end.

schedule_stabilize() ->
    timer:send_interval(?Stabilize, self(), stabilize).

request(Peer, Predecessor) ->
    case Predecessor of
        nil ->
            Peer ! {status, nil};
        {Pkey, Ppid} ->
            Peer ! {status, {Pkey, Ppid}}
    end.

notify({Nkey, Npid}, Id, Predecessor, Store) ->
    case Predecessor of
        nil ->
            Keep = handover(Id, Store, Nkey, Npid),
            {{Nkey, Npid}, Keep};

        {Pkey, _} ->
            case key:between(Nkey, Pkey, Id) of
                true ->
                    Keep = handover(Id, Store, Nkey, Npid),
                    {{Nkey, Npid}, Keep};

                false ->
                    {Predecessor, Store}
            end
    end.

handover(Id, Store, Nkey, Npid) ->
    {Rest, Keep} = storage:split(Id, Nkey, Store),
    Npid ! {handover, Rest},
    Keep.


create_probe(Id, {_, Spid}) ->
    T = erlang:system_time(microsecond),
    Spid ! {probe, Id, [Id], T}.

forward_probe(Ref, T, Nodes, Id, {_, Spid}) ->
    Spid ! {probe, Ref, [Id | Nodes], T}.

remove_probe(T, Nodes) ->
    Time = erlang:system_time(microsecond) - T,
    io:format("Probe completed in ~p us~n", [Time]),
    io:format("Nodes: ~p~n", [lists:reverse(Nodes)]).


add(Key,Value,Qref,Client,Id,{Pkey,_},{_, Spid},Store) ->
    case key:between(Key, Pkey,Id) of
        true ->
            Client ! {Qref, ok},
            storage:add(Key, Value, Store);
        false ->
            Spid ! {add,Key,Value,Qref,Client},
            Store end.

lookup(Key,Qref,Client,Id,{Pkey,_},{_, Spid},Store) ->
    case key:between(Key, Pkey,Id) of
        true ->
            Result = storage:lookup(Key, Store),
            Client ! {Qref, Result};
        false ->
            Spid ! {lookup,Key,Qref,Client}
            end.