%% ----------------------------------------------------------------------------
%%
%% vbisect: Variable Binary Dictionary data structure
%%
%% Copyright 2012-2014 (c) Trifork A/S.  All Rights Reserved.
%% http://trifork.com/ info@trifork.com
%%
%% This file is provided to you under the Apache License, Version 2.0 (the
%% "License"); you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
%% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
%% License for the specific language governing permissions and limitations
%% under the License.
%%
%% ----------------------------------------------------------------------------
-module(vbisect).
-author('Kresten Krab Thorup <krab@trifork.com>').

-export([
         is_vbisect/1, is_vbisect/2,
         data_version/1,
         from_orddict/1, to_orddict/1,
         from_gb_tree/1, to_gb_tree/1,
         find/2, find_geq/2,
         foldl/3, foldr/3, merge/3,
         dictionary_size_in_bytes/1,
         log_summary/1, log_summary/2, log_full/1
        ]).

-compile({inline, [skip_to_smaller_node/1, skip_to_bigger_node/3]}).

%% Uncomment these compile optimizations if you don't need to trace these functions.
%% In testing there didn't seem to be much difference with inline compilation here.
%% -compile({inline, [
%%                    find_node_smaller/3, find_node_bigger/5,
%%                    find_geq_node_smaller/4, find_geq_node_bigger/6
%%                   ]}).

-type key()     :: binary().
-type value()   :: binary().
-type bindict() :: binary().
-export_type([key/0, value/0, bindict/0]).


%% Vbisect macros contain only the comma-delimited binary
%% matching fields, so that in context the << ... >> are
%% required to make clear that a binary pattern-match occurs.

%% Magic prefix identifies Vbisect type, and allows up to
%% 255 versions of the implementation co-resident.
-define(V1, 1).
-define(MAGIC_V1, "vbs", ?V1:8/unsigned).
-define(MAX_DICT_ENTRIES, 16#ffffffff).

%% Fixed-size fields for computing positions in bytes.
-define(KEY_SIZE_IN_BYTES,        2).
-define(VALUE_SIZE_IN_BYTES,      4).
-define(DICT_PTR_SIZE_IN_BYTES,   4).

%% Fixed-size fields for constructing / destructuring binaries.
-define(KEY_SIZE_IN_BITS,        16).
-define(VALUE_SIZE_IN_BITS,      32).
-define(DICT_PTR_SIZE_IN_BITS,   32).
-define(DICT_ENTRIES_COUNT_BITS, 32).

%% Match or construct only the Vbisect header fields...
-define(MAKE_VBISECT_HDR(__Num_Entries),
        ?MAGIC_V1, __Num_Entries:?DICT_ENTRIES_COUNT_BITS/unsigned ).

%% Match to get the number of entries and the raw binary dictionary...
-define(MATCH_VBISECT_DATA(__Num_Entries, __Vbisect),
        ?MAKE_VBISECT_HDR(__Num_Entries), __Vbisect/binary ).

%% All fields inside the dictionary have a size preceding the actual value...
-define(KEY_ENTRY(__Key),
        __KeySize:?KEY_SIZE_IN_BITS/unsigned, __Key:__KeySize/binary).

-define(MAKE_KEY_ENTRY(__KeySize, __Key),
        __KeySize:?KEY_SIZE_IN_BITS/unsigned, __Key:__KeySize/binary).

-define(VALUE_ENTRY(__Value),
        __ValueSize:?VALUE_SIZE_IN_BITS/unsigned, __Value:__ValueSize/binary).

-define(MAKE_VALUE_ENTRY(__ValueSize, __Value),
        __ValueSize:?VALUE_SIZE_IN_BITS/unsigned, __Value:__ValueSize/binary).

-define(SMALLER_ENTRY(__Smaller),
        __SmallerSize:?DICT_PTR_SIZE_IN_BITS/unsigned, __Smaller:__SmallerSize/binary).

%% Except Bigger which falls last and thus consumes the rest of the binary.
-define(BIGGER_ENTRY(__Bigger), __Bigger/binary).

%% Deconstruct the raw binary dictionary which is formatted as
%% << Key, Smaller_Node, Value, Bigger_Node >> so that CPU cache
%% lines are pre-fetched when search descends SmallerNode. Also
%% note that Bigger is last and therefore does not need to waste
%% space storing a size field.
-define(MATCH_VBISECT_NODE(__Key, __Value, __Smaller, __Bigger),
        ?KEY_ENTRY     (__Key),
        ?SMALLER_ENTRY (__Smaller),
        ?VALUE_ENTRY   (__Value),
        ?BIGGER_ENTRY  (__Bigger)).


%% ===================================================================
%% API functions
%% ===================================================================

-spec is_vbisect(any()) -> boolean().
-spec is_vbisect(any(), pos_integer()) -> boolean().
-spec data_version(any()) -> ?V1 | error.

is_vbisect(BinDict) -> is_vbisect(BinDict, ?V1).
    
is_vbisect(<< ?MATCH_VBISECT_DATA(_, _) >>, ?V1) -> true;
is_vbisect(                              _,   _) -> false.

data_version(BinDict) ->
    case is_vbisect(BinDict) of
        false -> error;
        true  -> ?V1
    end.


-spec from_orddict(orddict:orddict()) -> bindict().
-spec to_orddict(bindict()) -> orddict:orddict().

%% Validate at least the first element of the list is {Key, Value}.
from_orddict([]) ->
    from_gb_tree(gb_trees:from_orddict([]));
from_orddict([{_Key, _Value} | _] = OrdDict)
  when is_binary(_Key), is_binary(_Value) ->
    from_gb_tree(gb_trees:from_orddict(OrdDict)).

to_orddict(BinDict) ->
    Fold_Fn = fun(Key, Value, Acc) -> [{Key, Value} | Acc] end,
    foldr(Fold_Fn, [], BinDict).


-spec from_gb_tree(gb_tree()) -> bindict().
-spec to_gb_tree  (bindict()) -> gb_tree().

from_gb_tree({Num_Entries, Node}) when Num_Entries =< ?MAX_DICT_ENTRIES ->
    {_BinSize, IOList} = encode_gb_node(Node),
    iolist_to_binary([ << ?MAKE_VBISECT_HDR(Num_Entries) >> | IOList ]).

to_gb_tree(<< ?MATCH_VBISECT_DATA(Num_Entries, Nodes) >> = _BinDict) ->
    {Num_Entries, to_gb_node(Nodes)}.


-spec find    (key(), bindict()) -> {ok, value()}                    | error.
-spec find_geq(key(), bindict()) -> {ok, Key::key(), Value::value()} | none.

find(Key, << ?MATCH_VBISECT_DATA(_Num_Entries, Nodes) >> = _BinDict) ->
    find_node(Key, Nodes).

%% Find largest Key + Value smaller than or equal to Key.
%% This is good for an inner node where key is the smallest key in the child node.
find_geq(Key, << ?MATCH_VBISECT_DATA(_Num_Entries, Nodes) >> = _BinDict) ->
    find_geq_node(Key, none, Nodes).


-type dict_fold_fn()  :: fun((Key::key(), Value::value(),     Acc::term()) -> term()).
-type dict_merge_fn() :: fun((Key::key(), Value1::value(), Value2::term()) -> term()).

-spec foldl(dict_fold_fn(),  term(),    bindict()) -> term().
-spec foldr(dict_fold_fn(),  term(),    bindict()) -> term().
-spec merge(dict_merge_fn(), bindict(), bindict()) -> bindict().

foldl(Fun, Acc, << ?MATCH_VBISECT_DATA(_Num_Entries, Nodes) >> = _BinDict) ->
    foldl_node(Fun, Acc, Nodes).

foldr(Fun, Acc, << ?MATCH_VBISECT_DATA(_Num_Entries, Nodes) >> = _BinDict) ->
    foldr_node(Fun, Acc, Nodes).

merge(Fun, BinDict1, BinDict2) ->
    OD1 = to_orddict(BinDict1),
    OD2 = to_orddict(BinDict2),
    OD3 = orddict:merge(Fun, OD1, OD2),
    from_orddict(OD3).


-spec dictionary_size_in_bytes(bindict()) -> pos_integer().

dictionary_size_in_bytes(<< ?MATCH_VBISECT_DATA(_Num_Entries, _Nodes) >> = BinDict) ->
    byte_size(BinDict).

%% Functions for logging information about vbisect instances.
%% They attempt to organize data in lines but leave caller to insert newlines.
-spec log_summary(bindict()) -> iolist().
-spec log_summary(bindict(), [key()]) -> iolist().
-spec log_full(bindict()) -> [binary() | {key(), value()}].

%% Display the number of entries and size of the dictionary in bytes.
log_summary(<< ?MATCH_VBISECT_DATA(Num_Entries, _Nodes) >> = BinDict) ->
     case Num_Entries of
         1 -> [<<"VBS1: 1 entry">>];
         _ -> [<<"VBS1: ">>, integer_to_binary(Num_Entries), <<" entries">>]
     end
        ++ [<<" (">>, integer_to_binary(dictionary_size_in_bytes(BinDict)), <<" bytes)">>].

%% Display summary plus the key properties requested.
log_summary(<< ?MATCH_VBISECT_DATA(_Num_Entries, _Nodes) >> = Bin_Dict, Important_Keys) ->
    Summary      = log_summary(Bin_Dict),
    Unique_Props = case Important_Keys of
                       [] -> <<>>;
                       [First | Rest] ->
                           [[First, <<": ">>, find(First, Bin_Dict)]
                               | [[<<", ">>, K, <<": ">>, find(K, Bin_Dict)] || K <- Rest]]
                   end,
    [Summary, <<" [ ">>, Unique_Props, <<" ]">>].

%% Display summary plus the full dictionary of keys and values in sorted order.
log_full(Bin_Dict) ->
    Summary = iolist_to_binary(log_summary(Bin_Dict)),
    Values  = orddict:to_list(to_orddict(Bin_Dict)),
    [Summary | Values].


%% ===================================================================
%% Support functions
%% ===================================================================

skip_to_smaller_node(KeySize) ->
    ?KEY_SIZE_IN_BYTES + KeySize.

skip_to_value(KeySize, SmallerSize) ->
    ?KEY_SIZE_IN_BYTES + KeySize + ?DICT_PTR_SIZE_IN_BYTES + SmallerSize.

skip_to_bigger_node(KeySize, ValueSize, SmallerSize) ->
    ?KEY_SIZE_IN_BYTES + KeySize + ?DICT_PTR_SIZE_IN_BYTES + SmallerSize
        + ?VALUE_SIZE_IN_BYTES + ValueSize.

%% Recursively encode gb_trees format as a binary tree.
encode_gb_node({Key, Value, Smaller, Bigger}) when is_binary(Key), is_binary(Value) ->
    {SmallerSize, IOSmaller} = encode_gb_node(Smaller),
    {BiggerSize,  IOBigger}  = encode_gb_node(Bigger),

    KeySize    = byte_size(Key),
    ValueSize  = byte_size(Value),
    NodeSize   = skip_to_bigger_node(KeySize, ValueSize, SmallerSize) + BiggerSize,
    NodeBinary = [
                  %% The Key in Size/Key format...
                  << ?MAKE_KEY_ENTRY(KeySize, Key),

                     %% The smaller node in Size/Smaller format...
                     SmallerSize:?DICT_PTR_SIZE_IN_BITS/unsigned >>, IOSmaller,

                  %% The Value for the Key in Size/Value format...
                  << ?MAKE_VALUE_ENTRY(ValueSize, Value) >>

                      %% The Bigger node is optimized as remaining binary with no size.
                      | IOBigger
                 ],
    {NodeSize, NodeBinary};
encode_gb_node(nil) -> {0, []}.

%% Convert from binary format to gb_trees [{Key, Value, Smaller, Bigger}, ...].
to_gb_node(<< ?MATCH_VBISECT_NODE(Key, Value, Smaller, Bigger) >>) ->
    {Key, Value, to_gb_node(Smaller), to_gb_node(Bigger)};
to_gb_node(<<>>) -> nil.

%% Recursively search nodes for a key, loading as little data to CPU cache as possible.
%% Avoid creating a sub-binary for Smaller and Bigger until we need them to proceed.
find_node(Key, << ?MATCH_VBISECT_NODE(HereKey, _, _, _) >> = Node) ->
    case HereKey of
        Candidate when Key  <  Candidate -> find_node_smaller (Key, Node, __KeySize);
        Candidate when Key  >  Candidate -> find_node_bigger  (Key, Node, __KeySize,
                                                               __ValueSize, __SmallerSize);
        Candidate when Key =:= Candidate -> find_value(Node, __KeySize, __SmallerSize)
    end;
find_node(_, <<>>) -> error.

find_value(Node, KeySize, SmallerSize) ->
    Skip_Size = skip_to_value(KeySize, SmallerSize),
    << _:Skip_Size/binary, ?VALUE_ENTRY(Value), _/binary >> = Node,
    Value.

%% Keep the same arg order as find_node to avoid overhead.
find_node_smaller(Key, Node, KeySize) ->
    Skip_Size = skip_to_smaller_node(KeySize),
    << _:Skip_Size/binary, ?SMALLER_ENTRY(Smaller), _/binary >> = Node,
    find_node(Key, Smaller).

%% Keep the same arg order as find_node to avoid overhead.
find_node_bigger(Key, Node, KeySize, ValueSize, SmallerSize) ->
    Skip_Size = skip_to_bigger_node(KeySize, ValueSize, SmallerSize),
    << _:Skip_Size/binary, ?BIGGER_ENTRY(Bigger) >> = Node,
    find_node(Key, Bigger).

%% Recursively search nodes for greatest lesser key, loading as little data to CPU cache as possible.
%% Avoid creating a sub-binary for Bigger unless we must search the right subtree.
find_geq_node(Key, Else, << ?MATCH_VBISECT_NODE(HereKey, _, _, _) >> = Node) ->
    case HereKey of
        Candidate when Key  <  Candidate, Else =/= none ->
            Else;
        Candidate when Key  <  Candidate ->
            find_geq_node_smaller(Key, Else, Node, __KeySize); 
        Candidate when Key  >  Candidate ->
            Reply = {ok, HereKey, find_value(Node, __KeySize, __SmallerSize)},
            find_geq_node_bigger(Key, Reply, Node, __KeySize, __ValueSize, __SmallerSize);
        Candidate when Key =:= Candidate ->
            {ok, HereKey, find_value(Node, __KeySize, __SmallerSize)}
    end;
find_geq_node(_, Else, <<>>) -> Else.

%% Keep the same arg order as find_geq_node to avoid overhead.
find_geq_node_smaller(Key, Else, Node, KeySize) ->
    Skip_Size = skip_to_smaller_node(KeySize),
    << _:Skip_Size/binary, ?SMALLER_ENTRY(Smaller), _/binary >> = Node,
    find_geq_node(Key, Else, Smaller).

%% Keep the same arg order as find_geq_node to avoid overhead.
find_geq_node_bigger(Key, Else, Node, KeySize, ValueSize, SmallerSize) ->
    Skip_Size = skip_to_bigger_node(KeySize, ValueSize, SmallerSize),
    << _:Skip_Size/binary, ?BIGGER_ENTRY(Bigger) >> = Node,
    find_geq_node(Key, Else, Bigger).


foldl_node(Fun, Acc, << ?MATCH_VBISECT_NODE(Key, Value, Smaller, Bigger) >> ) ->
    Acc1 = foldl_node(Fun, Acc, Smaller),
    Acc2 = Fun(Key, Value, Acc1),
    foldl_node(Fun, Acc2, Bigger);
foldl_node(_Fun, Acc, <<>>) -> Acc.


foldr_node(Fun, Acc, << ?MATCH_VBISECT_NODE(Key, Value, Smaller, Bigger) >> ) ->
    Acc1 = foldr_node(Fun, Acc, Bigger),
    Acc2 = Fun(Key, Value, Acc1),
    foldr_node(Fun, Acc2, Smaller);
foldr_node(_Fun, Acc, <<>>) -> Acc.
