%% -------------------------------------------------------------------
%%
%% riak_core: Core Riak Application
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

-module(riak_core_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    %% Don't add our system_monitor event handler here.  Instead, let
    %% riak_core_sysmon_minder start it, because that process can act
    %% on any handler crash notification, whereas we cannot.

    %% Validate that the ring state directory exists
    riak_core_util:start_app_deps(riak_core),
    RingStateDir = app_helper:get_env(riak_core, ring_state_dir),
    case filelib:ensure_dir(filename:join(RingStateDir, "dummy")) of
        ok ->
            ok;
        {error, RingReason} ->
            error_logger:error_msg(
              "Ring state directory ~p does not exist, "
              "and could not be created. (reason: ~p)\n",
              [RingStateDir, RingReason]),
            throw({error, invalid_ring_state_dir})
    end,

    %% Register our cluster_info app callback modules, with catch if
    %% the app is missing or packaging is broken.
    catch cluster_info:register_app(riak_core_cinfo_core),

    %% add these defaults now to supplement the set that may have been
    %% configured in app.config
    riak_core_bucket:append_bucket_defaults(
      [{n_val,3},
       {allow_mult,false},
       {last_write_wins,false},
       {precommit, []},
       {postcommit, []},
       {chash_keyfun, {riak_core_util, chash_std_keyfun}}]),

    %% Spin up the supervisor; prune ring files as necessary
    riak_core_ring_manager:prune_ringfiles(),
    Ring = case riak_core_ring_manager:find_latest_ringfile() of
        {ok, RingFile} ->
            riak_core_ring_manager:read_ringfile(RingFile);
        {error, not_found} ->
            error_logger:warning_msg("No ring file available.\n"),
            Ring0 = riak_core_ring:fresh(),
            riak_core_ring_manager:do_write_ringfile(Ring0),
            Ring0;                   
        {error, Reason1} ->
            error_logger:error_msg("Failed to load ring file: ~p\n",
                                   [Reason1]),
            throw({error, Reason1})
    end,
    riak_core_ring_manager:set_ring_global(Ring),
    case riak_core_sup:start_link() of
        {ok, Pid} ->
            ok = riak_core_ring_events:add_guarded_handler(riak_core_ring_handler, []),
            riak_core_ring_manager:set_my_ring(Ring),
            riak_core_node_watcher:service_up(riak_core, Pid),
            {ok, Pid};
        {error, Reason2} ->
            {error, Reason2}
    end.

stop(_State) ->
    ok.
