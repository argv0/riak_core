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

%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
-module(riak_core_handoff_manager).
-behaviour(gen_server).
-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).
-export([add_exclusion/2, get_handoff_lock/1, get_exclusions/1]).
-export([remove_exclusion/2]).
-export([release_handoff_lock/2]).
-export([install_view/4, handoff_completed/4]).
-export([test/0]).

-record(state, {status = up,
                ring,
                ring_hash,
                views = [],
                peer_views = [],
                rings = [],
                excl=ordsets:new(), 
                global_xfers=dict:new(),
                pending_xfers=dict:new(),
                bcast_tref,
                bcast_mod = {gen_server, abcast}}).

-record(pxfer, {view,
                index :: non_neg_integer(),
                from_node :: node(),
                to_node :: node()}).
test() ->
    {ok, R} = riak_core_ring_manager:get_my_ring(),
    I = riak_core_ring:random_other_index(R),
    NewRing = riak_core_ring:transfer_node(I, node(), R),
    riak_core_ring_manager:set_my_ring(NewRing).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

install_view(OldHash, NewHash, Transfers, NewRing) ->
    gen_server:call(?MODULE, {install_view, OldHash, NewHash, Transfers, NewRing}).

handoff_completed(Mod, Idx, From, To) ->
    gen_server:cast(?MODULE, {handoff_completed, Mod, Idx, From, To}).


init([]) ->
    process_flag(trap_exit, true),
    watch_for_ring_events(),
    watch_for_node_events(),
    {Hash, Ring} = riak_core_ring_manager:get_ring_info(),
    {ok, schedule_broadcast(#state{excl=ordsets:new(), ring=Ring, ring_hash=Hash})}.

add_exclusion(Module, Index) ->
    gen_server:cast(?MODULE, {add_exclusion, {Module, Index}}).

remove_exclusion(Module, Index) ->
    gen_server:cast(?MODULE, {del_exclusion, {Module, Index}}).    

get_exclusions(Module) ->
    gen_server:call(?MODULE, {get_exclusions, Module}, infinity).

get_handoff_lock(LockId) ->
    TokenCount = app_helper:get_env(riak_core, handoff_concurrency, 500),
    get_handoff_lock(LockId, TokenCount).

get_handoff_lock(_LockId, 0) ->
    {error, max_concurrency};
get_handoff_lock(LockId, Count) ->
    case global:set_lock({{handoff_token, Count}, {node(), LockId}}, [node()], 0) of
        true ->
            {ok, {handoff_token, Count}};
        false ->
            get_handoff_lock(LockId, Count-1)
    end.    

release_handoff_lock(LockId, Token) ->
    global:del_lock({{handoff_token,Token}, {node(), LockId}}, [node()]).
    
handle_call({get_exclusions, Module}, _From, State=#state{excl=Excl}) ->
    Reply =  [I || {M, I} <- ordsets:to_list(Excl), M =:= Module],
    {reply, {ok, Reply}, State};
handle_call({install_view, OldHash, NewHash, Transfers, _NewRing}, _From, State) ->
    case handle_install_view(OldHash, NewHash, Transfers, _NewRing, State) of
        ignore ->
            io:format("trying to install existing view~n"),
            {reply, ignored, State};
        {new_view, NewState} ->
            broadcast(nodes(), NewState),
            io:format("installing new view: ~p~n", [NewHash]),
            {reply, ok, NewState}
    end.

handle_cast({handoff_completed, _Mod, Index, FromNode, ToNode}, State) ->
    io:format("completed: ~p~n", [{Index,FromNode,ToNode}]),
    io:format("views: ~p~n", [State#state.views]),
    F = fun(Ring, {Idx, To}) ->
                {new_ring, riak_core_ring:transfer_node(Idx, To, Ring)}
        end,
    {ok, _NewRing} = riak_core_ring_manager:ring_trans(F, {Index, ToNode}),
    {noreply, State};
handle_cast({del_exclusion, {Mod, Idx}}, State=#state{excl=Excl}) ->
    {noreply, State#state{excl=ordsets:del_element({Mod, Idx}, Excl)}};
handle_cast({add_exclusion, {Mod, Idx}}, State=#state{excl=Excl}) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    riak_core_ring_events:ring_update(Ring),
    {noreply, State#state{excl=ordsets:add_element({Mod, Idx}, Excl)}};
handle_cast({ring_update, _R}, State) ->
    %% Ring has changed; determine what peers are new to us
    %% and broadcast out current status to those peers.
    {Hash, _} = riak_core_ring_manager:get_ring_info(),
    io:format("~p: ring_update.  current view: ~p~n",
              [?MODULE, {Hash, riak_core_ring:membership_hash(_R), State#state.ring_hash}]),
    %%Peers0 = ordsets:from_list(riak_core_ring:all_members(R)),
    %%Peers = ordsets:del_element(node(), Peers0),
    %%S2 = peers_update(Peers, State),
    {noreply, State};
handle_cast({node_update, _N}, State) ->
    %% Ring has changed; determine what peers are new to us
    %% and broadcast out current status to those peers.
    {Hash, _} = riak_core_ring_manager:get_ring_info(),
    io:format("~p: node_update.  hash=~p~n", [?MODULE, Hash]),
    %_Peers0 = ordsets:from_list(riak_core_ring:all_members(R)),
    %_Peers = ordsets:del_element(node(), Peers0),
    %%S2 = peers_update(Peers, State),
    {noreply, State};
handle_cast({down, Node}, State) ->
    io:format("~p: node ~p is down~n", [?MODULE, Node]),
    {noreply, State};
handle_cast({state, Node, RingHash, Views, _GlobalXfers, _PendingXfers}, State) ->
    io:format("~n~n~p: node: ~p~n", [?MODULE, Node]),
    io:format("~p: ringhash: ~p~n", [?MODULE, RingHash]),
    %io:format("~p: globalxfers: ~p~n", [?MODULE, GlobalXfers]),
    %io:format("~p: pendingxfers: ~p~n", [?MODULE, PendingXfers]),
    NewState = update_peer_views(Node, RingHash, Views, State),
    io:format("~p: view: ~p, ~p~n", [?MODULE, Node, NewState#state.peer_views]),
    {noreply, NewState}.

handle_info({gen_event_EXIT, H, _}, State) ->
    %% Ring event handler has been removed for some reason; re-register
    io:format("event handler ~p exited~n", [H]),
    watch_for_ring_events(),
    watch_for_node_events(),
    {noreply, State};
handle_info(broadcast, State) ->
    % XXX hack
    S2 = broadcast(nodes(), State),
    {noreply, S2};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

update_peer_views(Node, _RingHash, Views, State) ->
    %Old = ordsets:from_list(proplists:get_value(Node, State#state.peer_views, [])),
    New = ordsets:from_list(Views),
    %Added = ordsets:subtract(New, Old),
    %Deleted = ordsets:subtract(Old, New),
    %Unchanged = ordsets:intersection(New, Old),
    %io:format("New: ~p~n", [New]),
    %io:format("Added: ~p~n", [Added]),
    %io:format("Deleted: ~p~n", [Deleted]),
    %io:format("Unchanged: ~p~n", [Unchanged]),
    State#state{peer_views=[{Node, New}|proplists:delete(Node, State#state.peer_views)]}.


handle_install_view(0,0,_,_,_) ->
    ignore;
handle_install_view(OldHash,NewHash,_,NewRing,State) when OldHash =:= NewHash ->
    io:format("INSTALL VIEW CHANGING RING: ~p:~p~n", [OldHash,NewHash]),
    {ok, NewRing} = riak_core_ring_manager:ring_trans(fun(_, _) -> {new_ring, NewRing} end, []),
    {new_view, State#state{ring=NewRing, ring_hash=NewHash}};
handle_install_view(OldHash, NewHash, Transfers, _NewRing, State) ->
    View = {OldHash, NewHash},
    case proplists:get_value(View, State#state.views) of
        undefined ->
            ViewXfers = [#pxfer{view=View, index=Index, from_node=FromNode,
                                to_node=ToNode}
                         || {Index, FromNode, ToNode} <- Transfers],
            
            case [{I#pxfer.index, I#pxfer.from_node, I#pxfer.to_node} 
                  || I <- ViewXfers] of
                         %%, I#pxfer.from_node =:= node()] of
                [] ->
                    io:format("no transfers~n"),
                    ignore;
                [{_Idx, _From, _To}|_Rest]=T ->
                    io:format("transfers: ~p~n", [T]),
                    do_transfers(T),
                    %NewRing = riak_core_ring:transfer_node(Idx, To, State#state.ring),
                    %riak_core_ring_manager:set_my_ring(NewRing),
                    %R = gen_server:cast({riak_core_gossip, From},{reconcile_ring, NewRing}),
                    %io:format("rpc result: ~p~n", [R]),
                    %R
                    ok
            end,
            {new_view, State#state{ring=_NewRing, ring_hash=NewHash, 
                                   views=[{View, ViewXfers}|State#state.views]}};
        _ ->
            ignore
    end.

do_transfers([]) ->
    ok;
do_transfers([{Idx,From,To}|Rest]) when From =:= node() ->
    {ok, Pid} = riak_core_vnode_master:get_vnode_pid(Idx, riak_kv_vnode),
    riak_core_vnode:handoff_to(Pid, To),
    do_transfers(Rest);
do_transfers([{Idx,From,To}|Rest]) ->
    io:format("ignoring not-mine xfer: ~p,~p,~p~n", [Idx, From, To]),
    do_transfers(Rest).
    
          
watch_for_ring_events() ->
    Self = self(),
    Fn = fun(R) ->
                 gen_server:cast(Self, {ring_update, R})
         end,
    riak_core_ring_events:add_sup_callback(Fn).

watch_for_node_events() ->
    Self = self(),
    Fn = fun(R) ->
                 gen_server:cast(Self, {node_update, R})
         end,
    riak_core_node_watcher_events:add_sup_callback(Fn).

broadcast(Nodes, State) ->
    Msg = case (State#state.status) of
        up ->
           {state, node(), State#state.ring_hash, 
            [V || {V, _} <- State#state.views],
            dict:to_list(State#state.global_xfers),
            dict:to_list(State#state.pending_xfers)};
        down ->
           {down, node()}
    end,
    {Mod, Fn} = State#state.bcast_mod,
    Mod:Fn(Nodes, ?MODULE, Msg),
    schedule_broadcast(State).

schedule_broadcast(State) ->
    case (State#state.bcast_tref) of
        undefined ->
            ok;
        OldTref ->
            erlang:cancel_timer(OldTref)
    end,
    Interval = app_helper:get_env(riak_core, gossip_interval),
    Tref = erlang:send_after(Interval, self(), broadcast),
    State#state { bcast_tref = Tref }.

