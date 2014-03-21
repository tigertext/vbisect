%% ----------------------------------------------------------------------------
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
-module(vbisect_SUITE).
-author('jay@duomark.com').

-export([
         all/0, groups/0,
         init_per_suite/1, end_per_suite/1,
         init_per_group/1, end_per_group/1
        ]).

%% Actual tests
-export([
         check_make_vbisect/1,
         check_use_vbisect/1,
         check_lookup_speed/1
        ]).

-include("vbisect_common_test.hrl").

all() -> [{group, functionality}, {group, performance}].

groups() -> [
             {functionality, [sequence], [{vbisect, [sequence],
                                           [check_make_vbisect, check_use_vbisect]}]},
             {performance,   [sequence], [{vbisect, [sequence],
                                           [check_lookup_speed]}]}
            ].

init_per_suite(Config) -> Config.
end_per_suite(_Config) -> ok.

init_per_group(Config) -> Config.
end_per_group(_Config) -> ok.
     

%% Test Module is ?TM
-define(TM, vbisect).

-spec check_make_vbisect(config()) -> ok.
check_make_vbisect(_Config) ->

    ct:log("Non-binary is not a vbisect, nor can one be made from non-binaries"),
    Test_Non_Binary
        = ?FORALL({Key, Value}, {any(), any()},
                  ?IMPLIES(not is_binary(Key) orelse not is_binary(Value),
                           begin
                               false = ?TM:is_vbisect(Key),
                               false = ?TM:is_vbisect(Value),
                               error = ?TM:data_version(Key),
                               error = ?TM:data_version(Value),
                               true = try ?TM:from_orddict([{Key, Value}])
                                      catch error:function_clause -> true
                                      end
                           end
                          )),
    true = proper:quickcheck(Test_Non_Binary, ?PQ_NUM(10)),

    ct:log("Make a vbisect and verify it is the right type"),
    Test_Make
        = ?FORALL({Key, Value}, {?TM:key(), ?TM:value()},
                  begin
                      Bin_Dict = vbisect:from_orddict([{Key, Value}]),
                      true  = ?TM:is_vbisect(Bin_Dict),
                      true  = ?TM:is_vbisect(Bin_Dict, 1),
                      false = ?TM:is_vbisect(Bin_Dict, 2),
                      1 = ?TM:data_version(Bin_Dict),
                      1 =:= ?TM:size(Bin_Dict)
                  end),
    true = proper:quickcheck(Test_Make, ?PQ_NUM(10)).
    

-spec check_use_vbisect(config()) -> ok.
check_use_vbisect(_Config) ->

    ct:log("Get from an empty vbisect should always fail"),
    VB_Empty = ?TM:from_orddict([]),
    Test_Empty
        = ?FORALL(Key, ?TM:key(),
                  (error =:= ?TM:find(Key, VB_Empty)
                    andalso none =:= ?TM:find_geq (Key, VB_Empty))),
    true = proper:quickcheck(Test_Empty, ?PQ_NUM(10)),
    
    ct:log("Get should return any field that is stored in a vbisect"),
    Test_Make
        = ?FORALL({Key, Value}, {?TM:key(), ?TM:value()},
                  begin
                      Bin_Dict = vbisect:from_orddict([{Key, Value}]),
                      ?TM:is_vbisect(Bin_Dict)
                          andalso ?TM:find(Key, Bin_Dict) =:= Value
                  end),
    true = proper:quickcheck(Test_Make, ?PQ_NUM(10)).


-spec check_lookup_speed(config()) -> ok.
check_lookup_speed(_Config) ->
    Start = 100000000000000,
    N = 100000,
    Keys = lists:seq(Start, Start+N),
    KeyValuePairs = lists:map(fun (I) -> {<<I:64/integer>>, <<255:8/integer>>} end, Keys),

    %% Will mostly be unique, if N is bigger than 10000
    ReadKeys = [<<(lists:nth(random:uniform(N), Keys)):64/integer>> || _ <- lists:seq(1, 1000)],
    B = ?TM:from_orddict(KeyValuePairs),
    erlang:garbage_collect(),
    time_reads(B, N, ReadKeys),
    ok.

time_reads(B, Size, ReadKeys) ->
    Parent = self(),
    spawn(
      fun() ->
              Runs = 20,
              Timings =
                  lists:map(
                    fun (_) ->
                            StartTime = now(),
                            _ = find_many(B, ReadKeys),
                            timer:now_diff(now(), StartTime)
                    end, lists:seq(1, Runs)),

              Rps = 1000000 / ((lists:sum(Timings) / length(Timings)) / 1000),
              error_logger:info_msg("Average over ~p runs, ~p keys in dict~n"
                                    "Average fetch ~p keys: ~p us, max: ~p us~n"
                                    "Average fetch 1 key: ~p us~n"
                                    "Theoretical sequential RPS: ~w~n",
                                    [Runs, Size, length(ReadKeys),
                                     lists:sum(Timings) / length(Timings),
                                     lists:max(Timings),
                                     (lists:sum(Timings) / length(Timings)) / length(ReadKeys),
                                     trunc(Rps)]),

              Parent ! done
      end),
    receive done -> ok after 1000 -> ok end.

-spec find_many(?TM:bindict(), [?TM:key()]) -> [?TM:value() | not_found].
find_many(B, Keys) ->
    lists:map(fun (K) -> ?TM:find(K, B) end, Keys).

