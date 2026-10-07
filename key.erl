-module(key).
-export([generate/0,between/3]).

% Generate a random key for the node
generate() ->
    rand:uniform(1000000000).

between(Key, From, To) when From < To ->
    Key > From andalso Key =< To;

between(Key, From, To) when From > To ->
    Key > From orelse Key =< To;

between(_, From, To) when From == To ->
    true.