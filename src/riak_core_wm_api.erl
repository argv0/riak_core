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

%% @doc This module provides a Webmachine resource that lists the
%%      URLs for other resources available on this host.
%%
%%      Links to Riak resources will be added to the Link header in
%%      the form:
%%```
%%         <URL>; rel="RESOURCE_NAME"
%%'''
%%      HTML output of this resource is a list of link tags like:
%%```
%%         <a href="URL">RESOURCE_NAME</a>
%%'''
%%      JSON output of this resource in an object with elements like:
%%```
%%         "RESOURCE_NAME":"URL"
%%'''
-module(riak_core_wm_api).
-export([
         init/1,
         resource_exists/2,
         content_types_provided/2,
%%         to_html/2,
         to_json/2
        ]).

-record(state, {}).
-include_lib("webmachine/include/webmachine.hrl").

init([]) ->
    {ok, #state{}}.

resource_exists(RD, State) ->
    {true, RD, State}.

%add_link_header(RD, State) ->
%    wrq:set_resp_header(
%      "Link",
%      string:join([ ["<",Uri,">; rel=\"",Resource,"\""]
%                    || {Resource, Uri} <- State ],
%                  ","),
%      RD).

content_types_provided(RD, State) ->
%%    {[{"text/html", to_html},{"application/json", to_json}], RD, State}.
    {[{"application/json", to_json}], RD, State}.

%to_html(RD, State) ->
%    {["<html><body><ul>",
%      [ ["<li><a href=\"", Uri, "\">", Resource, "</a></li>"]
%        || {Resource, Uri} <- State ],
%      "</ul></body></html>"],
%     RD, State}.

to_json(RD, State) ->
    {json_pp:print(mochijson2:encode({struct, [{ring, riak_core_wm_api_util:ring_to_json()}]})),
     RD, State}.

