%% -------------------------------------------------------------------
%%
%% riak_core: Core Riak Application
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------
-module(riak_core_wm_api_util).
-export([ring_to_json/0, ring_to_json/1, status_to_json/0, vnode_module_status_to_json/1]).

ring_to_json() ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    ring_to_json(Ring).

ring_to_json(Ring) ->
    {struct, 
     [
      {vclock, vclock_to_json(riak_core_ring:get_vclock(Ring))},
      {num_partitions, riak_core_ring:num_partitions(Ring)},
      {ring_owner, riak_core_ring:owner_node(Ring)},
      {ownership,partitions_to_json(riak_core_ring:all_owners(Ring), [])},
      {meta, meta_to_json(riak_core_ring:get_meta(Ring))}
     ]}.

status_to_json() ->
    [
      {connected_nodes, [node()|nodes()]},
      {running_apps, sets:to_list(sets:from_list([A || {A,_M} <- riak_core:vnode_modules()]))},
      {vnode_modules, vnode_modules_to_json(riak_core:vnode_modules(), [])}
    ].

vnode_module_status_to_json(VNodeMod) ->
    {ok, R} = riak_core_ring_manager:get_my_ring(),
    AllPids = riak_core_vnode_master:all_nodes(VNodeMod),
    Indexes = [element(2, riak_core_vnode:get_mod_index(P)) || P <- AllPids],
    Exclusions = element(2, riak_core_handoff_manager:get_exclusions(VNodeMod)),
    {struct, 
      [{list_to_binary(integer_to_list(I)), 
        {struct, 
         [
          {fallback, riak_core_ring:index_owner(R, I) /= node()},
          {handed_off, lists:member(I, Exclusions)}
         ]}}
       || I <- Indexes]}.

meta_to_json(_Meta) ->
    {struct, []}.

vnode_modules_to_json([], Acc) ->
    sets:to_list(sets:from_list(Acc));
vnode_modules_to_json([{_App, Mod}|T], Acc) ->
    vnode_modules_to_json(T, [Mod|Acc]).

vclock_to_json(VClock) ->
    vclock_to_json(vclock:all_nodes(VClock), VClock, []).

vclock_to_json([], _VClock, Acc) ->
    lists:reverse(Acc);
vclock_to_json([Node|T], VClock, Acc) ->
    vclock_to_json(T, VClock,
                   [[Node, 
                     vclock:get_counter(Node, VClock), 
                     vclock:get_timestamp(Node, VClock)]|Acc]).

partitions_to_json([], Acc) ->
    lists:reverse(Acc);
partitions_to_json([{Index,Node}|T], Acc) ->
    partitions_to_json(T, [[list_to_binary(integer_to_list(Index)), Node]|Acc]).



