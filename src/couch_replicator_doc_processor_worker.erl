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

-module(couch_replicator_doc_processor_worker).

-export([spawn_worker/3]).

-include("couch_replicator.hrl").

-import(couch_replicator_utils, [
    pp_rep_id/1
]).


% Spawn a worker which attempts to calculate replication id then add a
% replication job to scheduler. This function create a monitor to the worker
% a worker will then exit with the #doc_worker_result{} record within
% the specified Timeout period. A timeout is considered a `temporary_error`.
% Result will be sent as the `Reason` in the {'DOWN',...} message.
-spec spawn_worker({binary(), binary()}, #rep{}, non_neg_integer()) ->
    reference().
spawn_worker(Id, Rep, Timeout) ->
    {_Pid, WRef} = spawn_monitor(fun() -> worker_fun(Id, Rep, Timeout) end),
    WRef.


% Private functions

-spec worker_fun(db_doc_id(), #rep{}, sec()) -> no_return().
worker_fun(Id, Rep, Timeout) ->
    Fun = fun() ->
        try maybe_start_replication(Id, Rep) of
            Res ->
                exit(Res)
        catch
            throw:{filter_fetch_error, Reason} ->
                exit({temporary_error, Reason});
            _Tag:Reason ->
                exit({temporary_error, Reason})
        end
    end,
    {Pid, Ref} = spawn_monitor(Fun),
    receive
        {'DOWN', Ref, _, Pid, Result} ->
            exit(#doc_worker_result{id = Id, result = Result})
    after Timeout * 1000 ->
        erlang:demonitor(Ref, [flush]),
        exit(Pid, kill),
        {DbName, DocId} = Id,
        Msg = io_lib:format("Replication for db ~p doc ~p failed to start due "
            "to timeout after ~B seconds", [DbName, DocId, Timeout]),
        Result = {temporary_error, couch_util:to_binary(Msg)},
        exit(#doc_worker_result{id = Id, result = Result})
    end.


% Try to start a replication. Used by a worker. This function should return
% rep_start_result(), also throws {filter_fetch_error, Reason} if cannot fetch filter.
% It can also block for an indeterminate amount of time while fetching the
% filter.
maybe_start_replication(Id, RepWithoutId) ->
    Rep = couch_replicator_docs:update_rep_id(RepWithoutId),
    case maybe_add_job_to_scheduler(Id, Rep) of
    {ok, RepId} ->
        {ok, RepId};
    {temporary_error, Reason} ->
        {temporary_error, Reason};
    {permanent_failure, Reason} ->
        {DbName, DocId} = Id,
        couch_replicator_docs:update_failed(DbName, DocId, Reason),
        {perment_failure, Reason}
    end.


-spec maybe_add_job_to_scheduler(db_doc_id(), #rep{}) -> rep_start_result().
maybe_add_job_to_scheduler({_DbName, DocId}, Rep) ->
    RepId = Rep#rep.id,
    case couch_replicator_scheduler:rep_state(RepId) of
    nil ->
        case couch_replicator_scheduler:add_job(Rep) of
        ok ->
           ok;
        {error, already_added} ->
            couch_log:warning("replicator scheduler: ~p was already added", [Rep])
        end,
        {ok, RepId};
    #rep{doc_id = DocId} ->
        {ok, RepId};
    #rep{doc_id = null} ->
        Msg = io_lib:format("Replication `~s` specified by document `~s`"
            " already running as a transient replication, started via"
            " `_replicate` API endpoint", [pp_rep_id(RepId), DocId]),
        {temporary_error, couch_util:to_binary(Msg)};
    #rep{db_name = OtherDb, doc_id = OtherDocId} ->
        Msg = io_lib:format("Replication `~s` specified by document `~s`"
            " already started, triggered by document `~s` from db `~s`",
            [pp_rep_id(RepId), DocId, OtherDocId, mem3:dbname(OtherDb)]),
        {permanet_failure, couch_util:to_binary(Msg)}
    end.
