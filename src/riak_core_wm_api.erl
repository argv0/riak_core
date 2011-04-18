%% -------------------------------------------------------------------
%%
%% Riak: A lightweight, decentralized key-value store.
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
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
-module(riak_core_wm_api).
-export([
         init/1,
         resource_exists/2,
         content_types_provided/2,
%%         to_html/2,
         to_json/2
        ]).

-record(state, {type, item, master_name, boundary}).
-include_lib("webmachine/include/webmachine.hrl").

init([]) ->
    {ok, #state{}}.

resource_exists(RD, State) ->
    Type = wrq:path_info(itemtype, RD),
    Item = wrq:path_info(item, RD),
    resource_exists({Type, Item}, RD, State#state{type=Type, item=Item}).

resource_exists({"ring",_}, RD, State) ->
    {true, RD, State};
resource_exists({"status",_}, RD, State) ->
    {true, RD, State};
resource_exists({"events",_}, RD, State) ->
    {true, RD, State};
resource_exists({"vnode",VNodeMod}, RD, State) ->
    try 
        MasterName = list_to_existing_atom(VNodeMod ++ "_master"),
        case whereis(MasterName) of
            undefined ->
                {false, RD, State};
            _ ->
                {true, RD, State#state{master_name=MasterName}}
        end
    catch error:badarg ->
            {false, RD, State}
    end;
resource_exists({undefined,_}, RD, State) ->
    {true, RD, State};
resource_exists(_, RD, State) ->
    {false, RD, State}.

content_types_provided(RD, State) ->
%%    {[{"text/html", to_html},{"application/json", to_json}], RD, State}.
    {[{"application/json", to_json}], RD, State}.

%to_html(RD, State) ->
%    {["<html><body><ul>",
%      [ ["<li><a href=\"", Uri, "\">", Resource, "</a></li>"]
%        || {Resource, Uri} <- State ],
%      "</ul></body></html>"],
%     RD, State}.

to_json(RD, State=#state{type="events"}) ->
    Self = self(),
    RingFn = fun(R) ->
                     Self ! {ring_update, R}
             end,
    riak_core_ring_events:add_callback(RingFn),
    NodeFn = fun(S) ->
                     Self ! {service_update, S}
             end,
    riak_core_node_watcher_events:add_callback(NodeFn),
    Boundary = riak_core_util:unique_id_62(),
    RD1 = wrq:set_resp_header("Content-Type", "multipart/mixed;boundary=" ++ Boundary, RD),
    State1 = State#state{boundary=Boundary},
    {true, wrq:set_resp_body({stream, stream_events(RD1, State1)}, RD1), State1};
to_json(RD, State=#state{type=Type, item=Item}) ->
    JSON = mochijson2:encode(get_json({Type, Item}, State)),
    Body = case wrq:get_qs_value("pretty", "false", RD) of
        "true" -> json_pp:print(JSON);
        _ -> JSON
    end,
    {Body, RD, State}.


get_json({"ring", undefined}, #state{}) ->
    {struct, [{ring, riak_core_wm_api_util:ring_to_json()}]};
get_json({"ring", "status"}, #state{}) ->
    {struct, [{ready, true}]};
get_json({"status", _}, #state{}) ->
    {struct, riak_core_wm_api_util:status_to_json()};
get_json({"vnode", Item}, #state{}) ->
    riak_core_wm_api_util:vnode_module_status_to_json(list_to_existing_atom(Item)).

stream_events(RD, #state{boundary=Boundary}=State) ->
    receive
        {ring_update, R} ->
            {iolist_to_binary(["\r\n--", Boundary, "\r\n",
                               "Content-Type: application/json\r\n\r\n",
                               mochijson2:encode({struct, 
                                                  [{ring,
                                                    riak_core_wm_api_util:ring_to_json(R)}]})]),
             fun() -> stream_events(RD, State) end};
        {service_update, S} ->
            {iolist_to_binary(["\r\n--", Boundary, "\r\n",
                               "Content-Type: application/json\r\n\r\n",
                               mochijson2:encode(S)]),
             fun() -> stream_events(RD, State) end}
    end.
                     
%            Data = mochijson2:encode({struct, [{phase, PhaseId}, {data, Res}]}),
%            Body = ["\r\n--", State#state.boundary, "\r\n",
%                    "Content-Type: application/json\r\n\r\n",
%                    Data],
%            {iolist_to_binary(Body), fun() -> stream_mapred_results(RD, ReqId, State) end}



%            Body = ["\r\n--", State#state.boundary, "\r\n",
%                    "Content-Type: application/json\r\n\r\n",
%                    Data],
%            {iolist_to_binary(Body), fun() -> stream_mapred_results(RD, ReqId, State) end}
