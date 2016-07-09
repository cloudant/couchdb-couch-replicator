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

-module(couch_replicator_doc_processor).
-behaviour(couch_multidb_changes).

-export([start_link/0]).

% multidb changes callback
-export([db_created/2, db_deleted/2, db_found/2, db_change/3]).

% gen_server callbacks
-export([init/1, handle_call/3, handle_info/2, handle_cast/2,
         code_change/3, terminate/2]).

% Config_listener callback
-export([handle_config_change/5, handle_config_terminate/3]).

-include_lib("couch/include/couch_db.hrl").
-include("couch_replicator.hrl").

-import(couch_replicator_utils, [
    get_json_value/2,
    get_json_value/3
]).

-define(DEFAULT_INTERVAL_SEC, 15).
-define(DEFAULT_MAX_WORKERS, 25).
-define(DEFAULT_WORKER_TIMEOUT_SEC, 30).
-define(ERROR_MAX_BACKOFF_EXPONENT, 18).  % ~ 1.5 days on average

-define(TS_DAY_SEC, 86400).

-type filter_type() ::  nil | view | user | docids | mango.
-type repstate() :: complete | failed | unscheduled | error | scheduled.


-record(state, {
    docs :: ets:tid(),
    todoq :: ets:tid(),
    timer :: timer:tref(),
    interval :: sec(),
    workers :: non_neg_integer(),
    max_workers :: non_neg_integer(),
    worker_timeout :: sec()
}).

-record(rdoc, {
    id :: db_doc_id(),
    state :: repstate(),
    rep :: #rep{} | nil,
    rid :: rep_id() | nil,
    filter :: filter_type(),
    info :: binary() | nil,
    errcnt :: non_neg_integer(),
    ts :: sec(),
    worker :: reference() | nil
}).



% couch_multidb_changes API callbacks

db_created(DbName, Server) ->
    couch_stats:increment_counter([couch_replicator, docs, dbs_created]),
    couch_replicator_docs:ensure_rep_ddoc_exists(DbName),
    Server.


db_deleted(DbName, Server) ->
    couch_stats:increment_counter([couch_replicator, docs, dbs_deleted]),
    ok = gen_server:call(?MODULE, {clean_up_replications, DbName}, infinity),
    Server.


db_found(DbName, Server) ->
    couch_stats:increment_counter([couch_replicator, docs, dbs_found]),
    couch_replicator_docs:ensure_rep_ddoc_exists(DbName),
    Server.


db_change(DbName, {ChangeProps} = Change, Server) ->
    couch_stats:increment_counter([couch_replicator, docs, db_changes]),
    try
        ok = process_change(DbName, Change)
    catch
    _Tag:Error ->
        {RepProps} = get_json_value(doc, ChangeProps),
        DocId = get_json_value(<<"_id">>, RepProps),
        couch_replicator_docs:update_failed(DbName, DocId, Error)
    end,
    Server.


% Private helpers for multidb changes API, these updates into the doc
% processor gen_server

process_change(DbName, {Change}) ->
    {RepProps} = JsonRepDoc = get_json_value(doc, Change),
    DocId = get_json_value(<<"_id">>, RepProps),
    Owner = couch_replicator_clustering:owner(DbName, DocId),
    Id = {DbName, DocId},
    case {Owner, get_json_value(deleted, Change, false)} of
    {_, true} ->
        ok = process_removed(Id);
    {unstable, false} ->
        couch_log:notice("Not starting '~s' as cluster is unstable", [DocId]);
    {ThisNode, false} when ThisNode =:= node() ->
        case get_json_value(<<"_replication_state">>, RepProps) of
        undefined ->
            ok = process_updated(Id, JsonRepDoc);
        <<"triggered">> ->
            couch_replicator_docs:remove_state_fields(DbName, DocId),
            ok = process_updated(Id, JsonRepDoc);
        <<"completed">> ->
            ok = process_completed(Id);
        <<"error">> ->
            % Handle replications started from older versions of replicator
            % which wrote transient errors to replication docs
            couch_replicator_docs:remove_state_fields(DbName, DocId),
            ok = process_updated(Id, JsonRepDoc);
        <<"failed">> ->
            Reason = get_json_value(<<"_replication_state_reason">>, RepProps),
            ok = process_failed(Id, Reason)
        end;
    {Owner, false} ->
        ok
    end,
    ok.


process_completed(Id) ->
    gen_server:call(?MODULE, {completed, Id}, infinity).


process_failed(Id, Reason) ->
    gen_server:call(?MODULE, {failed, Id, Reason}, infinity).


process_removed(Id) ->
    gen_server:call(?MODULE, {removed, Id}, infinity).


process_updated({DbName, _DocId} = Id, JsonRepDoc) ->
    % Parsing replication doc (but not calculating the id) could throw an
    % exception which would indicate this document is malformed. This exception
    % should propagate to db_change function and will be recorded as permanent
    % failure in the document. User will have to delete and re-create the document
    % to fix the problem.
    Rep0 = couch_replicator_docs:parse_rep_doc_without_id(JsonRepDoc),
    Rep = Rep0#rep{db_name = DbName},
    Filter = case couch_replicator_filters:parse(Rep#rep.options) of
    {ok, nil} ->
        nil;
    {ok, {user, _FName, _QP}} ->
        user;
    {ok, {view, _FName, _QP}} ->
        view;
    {ok, {docids, _DocIds}} ->
        docids;
    {ok, {mango, _Selector}} ->
        mango;
    {error, FilterError} ->
        throw(FilterError)
    end,
    gen_server:call(?MODULE, {updated, Id, Rep, Filter}, infinity).


% Doc processor gen_server API and callbacks

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [],  []).


init([]) ->
    random:seed(os:timestamp()),
    Interval = config:get_integer("replicator",
        "doc_processor_interval", ?DEFAULT_INTERVAL_SEC),
    MaxWorkers = config:get_integer("replicator",
        "doc_processor_max_workers", ?DEFAULT_MAX_WORKERS),
    WorkerTimeout = config:get_integer("replicator",
        "doc_processor_worker_timeout", ?DEFAULT_WORKER_TIMEOUT_SEC),
    {ok, Timer} = timer:send_after(Interval, reschedule),
    Docs = ets:new(?MODULE, [{keypos, #rdoc.id}]),
    TodoQ = ets:new(?MODULE, [ordered_set]),
    {ok, #state{
        docs = Docs,
        todoq = TodoQ,
        timer = Timer,
        interval = Interval,
        max_workers = MaxWorkers,
        worker_timeout = WorkerTimeout,
        workers = 0
    }}.


terminate(_Reason, _State) ->
    ok.


handle_call({updated, Id, Rep, Filter}, _From, State) ->
    {reply, ok,  updated_doc(Id, Rep, Filter, State)};

handle_call({completed, Id}, _From, #state{docs = Docs} = State) ->
    ok = update_terminal_ets(Docs, Id, completed, nil),
    ok = todoq_remove_doc(Id, State#state.todoq),
    {reply, ok, State};

handle_call({failed, Id, Reason}, _From, #state{docs = Docs} = State) ->
    ok = update_terminal_ets(Docs, Id, failed, Reason),
    ok = todoq_remove_doc(Id, State#state.todoq),
    {reply, ok, State};

handle_call({removed, Id}, _From, State) ->
    {reply, ok, removed_doc(Id, State)};

handle_call({clean_up_replications, DbName}, _From, State) ->
    {reply, ok, removed_db(DbName, State)}.


handle_cast({set_interval, Interval}, State)
        when is_integer(Interval), Interval > 0 ->
    couch_log:notice("~p: interval set to ~B", [?MODULE, Interval]),
    {noreply, State#state{interval = Interval}};

handle_cast({set_max_workers, MaxWorkers}, State)
        when is_integer(MaxWorkers), MaxWorkers > 0 ->
    couch_log:notice("~p: max_workers set to ~B", [?MODULE, MaxWorkers]),
    {noreply, State#state{max_workers = MaxWorkers}};

handle_cast({set_worker_timeout, TimeoutSec}, State)
        when is_integer(TimeoutSec), TimeoutSec > 0 ->
    couch_log:notice("~p: worker_timeout set to ~B", [?MODULE, TimeoutSec]),
    {noreply, State#state{worker_timeout = TimeoutSec}};

handle_cast(Msg, State) ->
    {stop, {error, unexpected_message, Msg}, State}.


handle_info(reschedule, State) ->
    {noreply, reschedule(State)};

handle_info({'DOWN', Ref, _, _, #doc_worker_result{id = Id, result = Res}},
        State) ->
    {noreply, worker_returned(Ref, Id, Res, State)};

handle_info(Msg, State) ->
    {stop, {error, unexpected_info_message, Msg}, State}.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


% Doc processor gen_server private helper functions


% Update table with a terminal state (completed or failed)
-spec update_terminal_ets(ets:tid(), db_doc_id(), repstate(), any()) -> ok.
update_terminal_ets(Ets, Id, TermState, Info) ->
    Row = #rdoc{
        id = Id,
        state = TermState,
        info = Info,
        errcnt = 0,
        ts = now_sec(),
        worker = nil
    },
    true = ets:insert(Ets, Row),
    ok.

% Handle doc update -- add to doc records to  ets, then start a worker to try
% to turn into a replication. In most cases it will succeed quickly but for
% filtered replications or if there are duplicates, it could take longer
% (theoretically indefinitely) until a replication could be started.
-spec updated_doc(db_doc_id(), #rep{}, filter_type(), #state{}) -> #state{}.
updated_doc(Id, Rep, Filter, #state{docs = Docs, todoq = TodoQ} = State) ->
    Ts = now_sec(),
    Row = #rdoc{
        id = Id,
        state = unscheduled,
        rep = Rep,
        rid = nil,
        filter = Filter,
        info = nil,
        errcnt = 0,
        ts = Ts,
        worker = nil
    },
    true = ets:insert(Docs, Row),
    ok = todoq_enqueue(Ts, Id, TodoQ),
    maybe_start_worker(Id, State).


-spec worker_returned(reference(), db_doc_id(), rep_start_result(), #state{}) ->
    #state{}.
worker_returned(Ref, Id, {ok, RepId}, State) ->
    #state{workers = Workers, docs = Docs, todoq = TodoQ} = State,
    Total = ets:info(Docs, size),
    case ets:lookup(Docs, Id) of
    [#rdoc{worker = Ref} = Row] ->
        true = ets:insert(Docs, update_docs_row(Row, RepId, Total, TodoQ));
    _ ->
        ok  % doc could have been deleted, ignore
    end,
    State#state{workers = Workers - 1};

worker_returned(Ref, Id, {temporary_error, Reason}, State) ->
    #state{workers = Workers, docs = Docs, todoq = TodoQ} = State,
    case ets:lookup(Docs, Id) of
    [#rdoc{worker = Ref} = Row] ->
        ErrCnt = Row#rdoc.errcnt + 1,
        Ts = now_sec(),
        TryNext = next_try_error(Ts, ErrCnt, ?ERROR_MAX_BACKOFF_EXPONENT),
        NewRow = Row#rdoc{
            rid = nil,
            state = error,
            info = Reason,
            errcnt = ErrCnt,
            ts = Ts,
            worker = nil
        },
        true = ets:insert(Docs, NewRow),
        ok = todoq_enqueue(TryNext, Id, TodoQ);
    _ ->
        ok  % doc could have been deleted, ignore
    end,
    State#state{workers = Workers - 1};

worker_returned(Ref, Id, {permanent_failure, Reason}, State) ->
    #state{workers = Workers, docs = Docs, todoq = TodoQ} = State,
    case ets:lookup(Docs, Id) of
    [#rdoc{worker = Ref}] ->
        ok = update_terminal_ets(Docs, Id, failed, Reason),
        ok = todoq_remove_doc(Id, TodoQ);
    _ ->
        ok  % doc could have been deleted, ignore
    end,
    State#state{workers = Workers - 1}.


% Filtered replication id didn't change. Reschedule another one for the future.
-spec update_docs_row(#rdoc{}, rep_id(), non_neg_integer(), ets:tid()) ->
    #rdoc{}.
update_docs_row(#rdoc{rid = R, filter = user} = Row, R, Total, TodoQ) ->
    Ts = now_sec(),
    TryNext = next_try_filter(Ts, Total),
    ok = todoq_enqueue(TryNext,  Row#rdoc.id, TodoQ),
    Row#rdoc{state = scheduled, errcnt = 0, ts = Ts, worker = nil};

% Calculated new replication id for a filtered replication. Make sure
% to schedule another check as filter code could change. Replications starts
% could have been failing, so also clear error count.
update_docs_row(#rdoc{rid = nil, filter = user} = Row, RepId, Total, TodoQ) ->
    Ts = now_sec(),
    TryNext = next_try_filter(Ts, Total),
    ok = todoq_enqueue(TryNext, Row#rdoc.id, TodoQ),
    Row#rdoc{rid = RepId, state = scheduled, errcnt = 0, ts = Ts, worker = nil};

% Replication id of existing replication job with filter has changed.
% Remove old replication job from scheduler and schedule check to check for
% future changes.
update_docs_row(#rdoc{rid = OldRepId, filter = user} = Row, RepId, Total,
        TodoQ) ->
    ok = couch_replicator_scheduler:remove_job(OldRepId),
    Msg = io_lib:format("Replication id changed: ~p -> ~p", [OldRepId, RepId]),
    Ts = now_sec(),
    TryNext = next_try_filter(Ts, Total),
    ok = todoq_enqueue(TryNext, Row#rdoc.id, TodoQ),
    Row#rdoc{
        rid = RepId,
        state = scheduled,
        info = couch_util:to_binary(Msg),
        errcnt = 0,
        ts = Ts,
        worker = nil
     };

% Calculated new replication id for non-filtered replication.
update_docs_row(#rdoc{id = Id, rid = nil} = Row, RepId, _Total, TodoQ) ->
    ok = todoq_remove_doc(Id, TodoQ),
    Row#rdoc{
        rep = nil, % remove replication doc body, after this we won't needed any more
        rid = RepId,
        state = scheduled,
        info = nil,
        errcnt = 0,
        ts = now_sec(),
        worker = nil
     }.


% When to try again after so many errors. Uses random exponential backoff.
% With a minimum of 5 seconds and a maximum exponent passed in.
-spec next_try_error(sec(), non_neg_integer(), non_neg_integer()) -> sec().
next_try_error(TsSec, MaxExp, ErrCnt) ->
    Exp = min(ErrCnt, MaxExp),
    Range = 1 bsl Exp,
    TsSec + 5 + random:uniform(Range).


% When to try again checking if filter changed, even after replication has
% started. This value auto-scales with the size of ets table --
% more docs the larger the time interval. The scaling factor is 1 second
% for each 10 docs. Minimum is 30 seconds max is 1 day.
-spec next_try_filter(sec(), non_neg_integer()) -> sec().
next_try_filter(TsSec, DocsSize) ->
    Range = min(2 * (DocsSize / 10), ?TS_DAY_SEC),
    TsSec + 30 + random:uniform(Range).


% Document removed from db -- clear ets table and remove all scheduled jobs
-spec removed_doc(db_doc_id(), #state{}) -> #state{}.
removed_doc({DbName, DocId} = Id, #state{docs = Docs, todoq = TodoQ} = State) ->
    ets:delete(Docs, Id),
    ok = todoq_remove_doc(Id, TodoQ),
    RepIds = couch_replicator_scheduler:find_jobs_by_doc(DbName, DocId),
    lists:foreach(fun couch_replicator_scheduler:remove_job/1, RepIds),
    State.


% Whole db shard is gone -- remove all ets rows which and scheduled jobs
% linked to that db.
-spec removed_db(binary(), #state{}) -> #state{}.
removed_db(DbName, #state{docs = Docs, todoq = TodoQ} = State) ->
    DocsPat = #rdoc{id = {DbName, '_'}, _ = '_'},
    ets:match_delete(Docs, DocsPat),
    ok = todoq_remove_db(DbName, TodoQ),
    RepIds = couch_replicator_scheduler:find_jobs_by_dbname(DbName),
    lists:foreach(fun couch_replicator_scheduler:remove_job/1, RepIds),
    State.


% Spawn a worker process which will attempt to calculate a replication id, then
% start a replication. Returns a process monitor reference. The worker is
% guaranteed to exit with rep_start_result() type only, within a specified
% `TimeoutSec` time window.
-spec maybe_start_worker(db_doc_id(), #state{}) -> #state{}.
maybe_start_worker(Id, #state{workers = Workers, worker_timeout = Tout} = State) ->
    case ets:lookup(State#state.docs, Id) of
    [] ->
        State;
    [#rdoc{state = completed}] ->
        State;
    [#rdoc{state = failed}] ->
        State;
    [#rdoc{state = scheduled, filter = Filter}] when Filter =/= user ->
        State;
    [#rdoc{rep = Rep} = Doc] ->
        WRef = couch_replicato_doc_processor_worker:spawn_worker(Id, Rep, Tout),
        true = ets:insert(State#state.docs, Doc#rdoc{worker = WRef}),
        State#state{workers = Workers + 1}
    end.


-spec reschedule(#state{}) -> #state{}.
reschedule(#state{workers = Workers, max_workers = MaxWorkers} = State) ->
    CanStart = MaxWorkers - Workers,
    NewState = case CanStart > 0 of
    true ->
        maybe_start_workers(CanStart, State);
    false ->
        State
    end,
    {ok, cancel} = timer:cancel(NewState#state.timer),
    Interval = NewState#state.interval,
    {ok, Timer} = timer:send_after(Interval * 1000, reschedule),
    NewState#state{timer = Timer}.


-spec maybe_start_workers(non_neg_integer(), #state{}) -> #state{}.
maybe_start_workers(CanStart, #state{todoq = TodoQ} = State) ->
    Ids = todoq_dequeue(now_sec(), CanStart, TodoQ),
    lists:foldl(fun(Id, St) -> maybe_start_worker(Id, St) end, State, Ids).


% TodoQ  -- small implementation of a timeline using a sorted_set ets
% indexed by {Timestamp, ...}. Can enqueue items at a particular time,
% then dequeue items which are "expired" i.e. their happened earlier
% than a particular timestamp.

% Enqueue an item into the timeline
-spec todoq_enqueue(sec(), db_doc_id(), ets:tid()) -> ok.
todoq_enqueue(Timestamp, Id, TodoQ) ->
    true = ets:insert(TodoQ, {{Timestamp, Id}}),
    ok.


% Dequeue up to `Limit` items with timestamp which is less than the
% given timestamp.
-spec todoq_dequeue(sec(), non_neg_integer(), ets:tid()) -> [_].
todoq_dequeue(Timestamp, Limit, TodoQ) when is_integer(Limit), Limit > 0 ->
    % note: <<>> sorts higher than tuples
    MS = ets:fun2ms(fun({T}) when T < {Timestamp, <<>>} -> T end),
    case ets:select(TodoQ, MS, Limit) of
        '$end_of_table' ->
            [];
        {Match, _Cont} ->
            lists:usort([
                begin
                    true = ets:delete(TodoQ, {Ts, Id}),
                    Id
                end || {Ts, Id} <- Match
            ])
    end.

% Clear items from TodoQ when a document should not longer have scheduled
% future events associated with it.
-spec todoq_remove_doc(db_doc_id(), ets:tid()) -> ok.
todoq_remove_doc(Id, TodoQ) ->
    true = ets:match_delete(TodoQ, {{'_', Id}}),
    ok.


% Remove all items from timeline which reference a particular db shard
-spec todoq_remove_db(binary(), ets:tid()) -> ok.
todoq_remove_db(DbName, TodoQ) ->
    true = ets:match_delete(TodoQ, {{'_', {DbName, '_'}}}),
    ok.


% Use plain seconds for time accounting. This simplifies arithmetic especially
% when calculating future events. Traslating to milliseconds immediately on use
% if some functions accept that (send_after, after ...).
-spec now_sec() -> sec().
now_sec() ->
    calendar:datetime_to_gregorian_seconds(calendar:universal_time()).



handle_config_change("replicator", "doc_processor_interval", V, _, S) ->
    ok = gen_server:cast(S, {set_interval, list_to_integer(V)}),
    {ok, S};

handle_config_change("replicator", "doc_processor_max_workers", V, _, S) ->
    ok = gen_server:cast(S, {set_max_workers, list_to_integer(V)}),
    {ok, S};

handle_config_change("replicator", "doc_processor_worker_timeout", V, _, S) ->
    ok = gen_server:cast(S, {set_worker_timeout, list_to_integer(V)}),
    {ok, S};


handle_config_change(_, _, _, _, S) ->
    {ok, S}.


handle_config_terminate(_, stop, _) ->
    ok;

handle_config_terminate(Self, _, _) ->
    spawn(fun() ->
        timer:sleep(5000),
        config:listen_for_changes(?MODULE, Self)
    end).



-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

-define(DB, <<"db">>).
-define(DOC1, <<"doc1">>).
-define(DOC2, <<"doc2">>).
-define(R1, {"1", ""}).
-define(R2, {"2", ""}).


doc_processor_test_() ->
    {
        foreach,
        fun setup/0,
        fun teardown/1,
        [
            t_bad_change(),
            t_regular_change(),
            t_deleted_change(),
            t_triggered_change(),
            t_completed_change(),
            t_error_change(),
            t_failed_change(),
            t_change_for_different_node(),
            t_change_when_cluster_unstable(),
            t_already_running_same_docid(),
            t_already_running_transient(),
            t_already_running_other_db_other_doc(),
            t_already_running_other_doc_same_db()
        ]
    }.


% Can't parse replication doc, so should write failure state to document.
t_bad_change() ->
    ?_test(begin
        meck:expect(couch_replicator_docs, parse_rep_doc,
            fun(_) -> throw({bad_rep_doc, <<"bad">>}) end),
        ?assertEqual(acc, db_change(?DB, bad_change(), acc)),
        ?assert(updated_doc_with_failed_state())
    end).


% Regular change, parse to a #rep{} and then add job.
t_regular_change() ->
    ?_test(begin
        ?assertEqual(ok, process_update(?DB, change())),
        ?assert(added_job())
    end).


% Change is a deletion, and job is running, so remove job.
t_deleted_change() ->
    ?_test(begin
        meck:expect(couch_replicator_scheduler, find_jobs_by_doc,
            fun(?DB, ?DOC1) -> [#rep{id = ?R2}] end),
        ?assertEqual(ok, process_update(?DB, deleted_change())),
        ?assert(removed_job(?R2))
    end).


% Change is in `triggered` state. Remove legacy state and add job.
t_triggered_change() ->
    ?_test(begin
        ?assertEqual(ok, process_update(?DB, change(<<"triggered">>))),
        ?assert(removed_state_fields()),
        ?assert(added_job())
    end).

% Change is in `completed` state, so skip over it.
t_completed_change() ->
    ?_test(begin
        ?assertEqual(ok, process_update(?DB, change(<<"completed">>))),
        ?assert(did_not_remove_state_fields()),
        ?assert(did_not_add_job())
    end).


% Change is in `error` state. Remove legacy state and retry
% running the job. This state was used for transient erorrs which are not
% written to the document anymore.
t_error_change() ->
    ?_test(begin
        ?assertEqual(ok, process_update(?DB, change(<<"error">>))),
        ?assert(removed_state_fields()),
        ?assert(added_job())
    end).


% Change is in `failed` state. This is a terminal state and it will not
% be tried again, so skip over it.
t_failed_change() ->
    ?_test(begin
        ?assertEqual(ok, process_update(?DB, change(<<"failed">>))),
        ?assert(did_not_add_job())
    end).


% Normal change, but according to cluster ownership algorithm, replication belongs to
% a different node, so this node should skip it.
t_change_for_different_node() ->
   ?_test(begin
        meck:expect(couch_replicator_clustering, owner, 2, different_node),
        ?assertEqual(ok, process_update(?DB, change())),
        ?assert(did_not_add_job())
   end).


% Change handled when cluster is unstable (nodes are added or removed), so
% job is not added. A rescan will be triggered soon and change will be evaluated again.
t_change_when_cluster_unstable() ->
   ?_test(begin
       meck:expect(couch_replicator_clustering, owner, 2, unstable),
       ?assertEqual(ok, process_update(?DB, change())),
       ?assert(did_not_add_job())
   end).


% Replication is already running, with same doc id. Ignore change.
t_already_running_same_docid() ->
   ?_test(begin
       mock_already_running(?DB, ?DOC1),
       ?assertEqual(ok, process_update(?DB, change())),
       ?assert(did_not_add_job())
   end).


% There is a transient replication with same replication id running. Ignore change.
t_already_running_transient() ->
   ?_test(begin
       mock_already_running(null, null),
       ?assertEqual(ok, process_update(?DB, change())),
       ?assert(did_not_add_job())
   end).


% There is a duplicate replication potentially from a different db and doc.
% Write permanent failure to doc.
t_already_running_other_db_other_doc() ->
   ?_test(begin
       mock_already_running(<<"otherdb">>, ?DOC2),
       ?assertEqual(ok, process_update(?DB, change())),
       ?assert(did_not_add_job()),
       ?assert(updated_doc_with_failed_state())
   end).

% There is a duplicate replication potentially from same db and different doc.
% Write permanent failure to doc.
t_already_running_other_doc_same_db() ->
   ?_test(begin
       mock_already_running(?DB, ?DOC2),
       ?assertEqual(ok, process_update(?DB, change())),
       ?assert(did_not_add_job()),
       ?assert(updated_doc_with_failed_state())
   end).


% Test helper functions


setup() ->
    meck:expect(couch_log, info, 2, ok),
    meck:expect(couch_log, notice, 2, ok),
    meck:expect(couch_log, warning, 2, ok),
    meck:expect(couch_log, error, 2, ok),
    meck:expect(couch_replicator_clustering, owner, 2, node()),
    meck:expect(couch_replicator_scheduler, remove_job, 1, ok),
    meck:expect(couch_replicator_scheduler, add_job, 1, ok),
    meck:expect(couch_replicator_docs, remove_state_fields, 2, ok),
    meck:expect(couch_replicator_docs, update_failed, 3, ok),
    meck:expect(couch_replicator_docs, parse_rep_doc,
        fun({DocProps}) ->
            #rep{id = ?R1, doc_id = get_json_value(<<"_id">>, DocProps)}
        end).


teardown(_) ->
    meck:unload().


mock_already_running(DbName, DocId) ->
    meck:expect(couch_replicator_scheduler, rep_state,
         fun(RepId) -> #rep{id = RepId, doc_id = DocId, db_name = DbName} end).


removed_state_fields() ->
    meck:called(couch_replicator_docs, remove_state_fields, [?DB, ?DOC1]).


added_job() ->
    meck:called(couch_replicator_scheduler, add_job, [
        #rep{id = ?R1, db_name = ?DB, doc_id = ?DOC1}]).


removed_job(Id) ->
    meck:called(couch_replicator_scheduler, remove_job, [#rep{id = Id}]).


did_not_remove_state_fields() ->
    0 == meck:num_calls(couch_replicator_docs, remove_state_fields, '_').


did_not_add_job() ->
    0 == meck:num_calls(couch_replicator_scheduler, add_job, '_').


updated_doc_with_failed_state() ->
    1 == meck:num_calls(couch_replicator_docs, update_failed, '_').


change() ->
    {[
        {<<"id">>, ?DOC1},
        {doc, {[
            {<<"_id">>, ?DOC1},
            {<<"source">>, <<"src">>},
            {<<"target">>, <<"tgt">>}
        ]}}
    ]}.


change(State) ->
    {[
        {<<"id">>, ?DOC1},
        {doc, {[
            {<<"_id">>, ?DOC1},
            {<<"source">>, <<"src">>},
            {<<"target">>, <<"tgt">>},
            {<<"_replication_state">>, State}
        ]}}
    ]}.


deleted_change() ->
    {[
        {<<"id">>, ?DOC1},
        {<<"deleted">>, true},
        {doc, {[
            {<<"_id">>, ?DOC1},
            {<<"source">>, <<"src">>},
            {<<"target">>, <<"tgt">>}
        ]}}
    ]}.


bad_change() ->
    {[
        {<<"id">>, ?DOC2},
        {doc, {[
            {<<"_id">>, ?DOC2},
            {<<"source">>, <<"src">>}
        ]}}
    ]}.




-endif.
