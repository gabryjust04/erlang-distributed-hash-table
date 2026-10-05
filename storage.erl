-module(storage).
-export([create/0, add/3, lookup/2, split/3, merge/2]).

create() ->
    [].

add(Key, Value, Store) ->
    lists:keystore(Key, 1, Store, {Key, Value}).

lookup(Key, Store) ->
    case lists:keyfind(Key, 1, Store) of
        {Key, Value} -> {Key, Value};
        false -> false
    end.

split(From, To, Store) ->
    splitR(From, To, Store, [], []).

splitR(From, To, [{Key, Value} | Store], Updated, Rest) ->
    case key:between(Key, From, To) of
        true ->
            splitR(From, To, Store, [{Key, Value} | Updated], Rest);
        false ->
            splitR(From, To, Store, Updated, [{Key, Value} | Rest])
    end;
splitR(_From, _To, [], Updated, Rest) ->
    {Updated, Rest}.

merge([], Store) ->
    Store;
merge([{Key, Value} | Entries], Store) ->
    merge(Entries, add(Key, Value, Store)).