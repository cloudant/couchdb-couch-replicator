% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License. You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
% License for the specific language governing permissions and limitations under
% the License.

-module(couch_replicator_scheduler_job).
-behaviour(gen_server).
-vsn(1).

-include("couch_replicator_scheduler.hrl").
-include("couch_replicator.hrl").

%% public api
-export([start_link/1]).

%% gen_server callbacks
-export([init/1, terminate/2, code_change/3]).
-export([handle_call/3, handle_cast/2, handle_info/2]).

%% definitions
-define(LOWEST_SEQ, 0).
-define(DEFAULT_CHECKPOINT_INTERVAL, 30000).
-record(rep_state, {
    rep_details,
    source_name,
    target_name,
    source,
    target,
    history,
    checkpoint_history,
    start_seq,
    committed_seq,
    current_through_seq,
    seqs_in_progress = [],
    highest_seq_done = {0, ?LOWEST_SEQ},
    source_log,
    target_log,
    rep_starttime,
    src_starttime,
    tgt_starttime,
    timer, % checkpoint timer
    changes_queue,
    changes_manager,
    changes_reader,
    workers,
    stats = couch_replicator_stats:new(),
    session_id,
    source_db_compaction_notifier = nil,
    target_db_compaction_notifier = nil,
    source_monitor = nil,
    target_monitor = nil,
    source_seq = nil,
    use_checkpoints = true,
    checkpoint_interval = ?DEFAULT_CHECKPOINT_INTERVAL,
    type = db,
    view = nil
}).

start_link(#rep{} = Rep) ->
    gen_server:start_link(
      {global, {?MODULE, Rep#rep.id}},
      ?MODULE,
      Rep,
      []).


init(#rep{} = Rep) ->
    process_flag(trap_exit, true),
    {ok, #rep_state{rep_details = Rep}}.


handle_call(_Msg, _From, State) ->
    {noreply, State}.


handle_cast(_Msg, State) ->
    {noreply, State}.


handle_info(_Msg, State) ->
    {noreply, State}.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


terminate(shutdown, State) ->
   couch_log:notice("~p asked to shutdown", [id(State)]);

terminate(_Reason, _State) ->
    ok.


id(#rep{} = Rep) ->
    Rep#rep.id;

id(#rep_state{} = RepState) ->
    id(RepState#rep_state.rep_details).
