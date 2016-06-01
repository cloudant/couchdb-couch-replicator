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

-module(couch_replicator_scheduler).
-behaviour(gen_server).
-behaviour(config_listener).
-vsn(1).

-include("couch_replicator_scheduler.hrl").
-include("couch_replicator.hrl").

%% public api
-export([start_link/0, add_job/1, remove_job/1]).

%% gen_server callbacks
-export([init/1, terminate/2, code_change/3]).
-export([handle_call/3, handle_cast/2, handle_info/2]).
-export([format_status/2]).

%% config_listener callback
-export([handle_config_change/5, handle_config_terminate/3]).

%% definitions
-define(DEFAULT_MAX_JOBS, 100).
-define(DEFAULT_MAX_CHURN, 20).
-define(DEFAULT_SCHEDULER_INTERVAL, 60000).
-define(ERROR_RETRY_MILLISECONDS, 30000).
-record(state, {interval, max_jobs, churn, churn_time, max_churn}).

-record(job, {
    id :: job_id(),
    rep :: #rep{},
    pid :: pid(),
    state :: runnable | running | stopping | stopped | {error, term()},
    started_at :: erlang:timestamp(),
    stopped_at :: erlang:timestamp(),
    errored_at :: erlang:timestamp(),
    error_timeout :: erlang:timestamp() | undefined,
    error_retries :: integer(),
    timeout_until :: integer()
}).

%% public functions

-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


-spec add_job(#rep{}) -> ok | {error, already_added}.
add_job(#rep{} = Rep) when Rep#rep.id /= undefined ->
    Job = #job{
        id = Rep#rep.id,
        rep = Rep,
        state = runnable,
        started_at = {0, 0, 0},
        stopped_at = {0, 0, 0},
        errored_at = {0, 0, 0},
        error_timeout = undefined,
        error_retries = 0},
    gen_server:call(?MODULE, {add_job, Job}).


-spec remove_job(job_id()) -> ok.
remove_job(Id) ->
    gen_server:call(?MODULE, {remove_job, Id}).


%% gen_server functions

init(_) ->
    ?MODULE = ets:new(?MODULE, [named_table, {keypos, #job.id}]),
    ets:new(couch_scheduler_timeout_box, [named_table, ordered_set]),
    ets:new(couch_scheduler_running, [named_table, ordered_set]),
    ets:new(couch_scheduler_runnable, [named_table, ordered_set]),
    ok = config:listen_for_changes(?MODULE, self()),
    Interval = config:get_integer("replicator", "interval", ?DEFAULT_SCHEDULER_INTERVAL),
    MaxJobs = config:get_integer("replicator", "max_jobs", ?DEFAULT_MAX_JOBS),
    MaxChurn = config:get_integer("replicator", "max_churn", ?DEFAULT_MAX_CHURN),
    {ok, #state{interval = Interval, max_jobs = MaxJobs, churn = 0, churn_time = now(), max_churn = MaxChurn}}.


handle_call({add_job, Job}, _From, State) ->
    case add_job_int(Job) of
        true ->
            format_timeout({reply, ok, State});
        false ->
            format_timeout({reply, {error, already_added}, State})
    end;

handle_call({remove_job, Id}, _From, State) ->
    case job_by_id(Id) of
        {ok, Job} ->
            ok = job_remove(Job);
        undefined ->
            ok
    end,
    format_timeout({reply, ok, State});

handle_call(_, _From, State) ->
    format_timeout({noreply, State}).


handle_cast({set_max_jobs, MaxJobs}, State) when is_integer(MaxJobs), MaxJobs > 0 ->
    couch_log:notice("~p: max_jobs set to ~B", [?MODULE, MaxJobs]),
    format_timeout({noreply, State#state{max_jobs = MaxJobs}});

handle_cast({set_max_churn, MaxChurn}, State) when is_integer(MaxChurn), MaxChurn > 0 ->
    couch_log:notice("~p: max_churn set to ~B", [?MODULE, MaxChurn]),
    {noreply, State#state{max_churn = MaxChurn}};

handle_cast({set_interval, Interval}, State) when is_integer(Interval), Interval > 0 ->
    couch_log:notice("~p: interval set to ~B", [?MODULE, Interval]),
    format_timeout({noreply, State#state{interval = Interval}});

handle_cast(_, State) ->
    format_timeout({noreply, State}).


handle_info({'DOWN', _Ref, process, Pid, Reason}, State) ->
    case job_by_pid(Pid) of
        {ok, #job{}=Job} ->
            case Reason of
                normal ->
                    job_runnable(Job);
                {error, EReason} ->
                    true = ets:insert(?MODULE, Job),
                    job_error(Job, EReason)
            end,
            couch_log:notice("~p: Job ~p died with reason: ~p",
                             [?MODULE, Job#job.id, Reason]),
            format_timeout({noreply, State});
        {error, not_found} ->
            % removed in remove_job and should not be reinserted.
            format_timeout({noreply, State})
    end;

handle_info(_, State) ->
    {noreply, State}.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


terminate(_Reason, _State) ->
    ok.


format_status(_Opt, [_PDict, State]) ->
    [{max_jobs, State#state.max_jobs},
     {running_jobs, running_job_count()},
     {runnable_jobs, runnable_job_count()}].


%% config listener functions

handle_config_change("replicator", "max_jobs", V, _, Pid) ->
    ok = gen_server:cast(Pid, {set_max_jobs, list_to_integer(V)}),
    {ok, Pid};

handle_config_change("replicator", "max_churn", V, _, Pid) ->
    ok = gen_server:cast(Pid, {set_max_churn, list_to_integer(V)}),
    {ok, Pid};

handle_config_change("replicator", "interval", V, _, Pid) ->
    ok = gen_server:cast(Pid, {set_interval, list_to_integer(V)}),
    {ok, Pid};

handle_config_change(_, _, _, _, Pid) ->
    {ok, Pid}.


handle_config_terminate(_, stop, _) ->
    ok;

handle_config_terminate(Self, _, _) ->
    spawn(fun() ->
        timer:sleep(5000),
        config:listen_for_changes(?MODULE, Self)
    end).


%% private functions

-spec add_job_int(#job{}) -> boolean().
add_job_int(#job{} = Job) ->
    case ets:insert_new(?MODULE, Job) of
        true ->
            true = ets:insert(couch_scheduler_runnable, {{{0, 0, 0}, Job#job.id}}),
            true;
        false ->
            false
    end.


job_running(#job{state=runnable} = Job0) ->
    case couch_replicator_scheduler_sup:start_child(Job0#job.rep) of
        {ok, Child} ->
            StartedAt = os:timestamp(),
            monitor(process, Child),
            gen_server:cast(Child, start),
            Job1 = Job0#job{
                pid = Child,
                errored_at = undefined,
                error_retries = 0,
                started_at = StartedAt,
                state=running
            },
            true = ets:insert(?MODULE, Job1),
            true = ets:insert(couch_scheduler_running, {{StartedAt, Job1#job.id}}),
            true = ets:delete(couch_scheduler_runnable, {Job1#job.stopped_at, Job1#job.id}),
            couch_log:notice("~p: Job ~p started as ~p",
                [?MODULE, Job1#job.id, Job1#job.pid]),
            Job1;
        {error, Reason} ->
            couch_log:notice("~p: Job ~p failed to start for reason ~p",
                [?MODULE, Job0, Reason]),
            job_error(Job0, Reason)
    end.


job_runnable(#job{state=running} = Job0) ->
    StoppedAt = os:timestamp(),
    gen_server:cast(Job0#job.pid, stop),
    supervisor:terminate_child(couch_scheduler_sup, Job0#job.pid),
    Job1 = Job0#job{pid = undefined, stopped_at = StoppedAt, state=runnable},
    ets:insert(?MODULE, Job1),
    true = ets:insert(?MODULE, Job1),
    true = ets:delete(couch_scheduler_running, {Job1#job.started_at, Job1#job.id}),
    true = ets:insert(couch_scheduler_runnable, {{StoppedAt, Job1#job.id}}),
    couch_log:notice("~p: Job ~p stopped as ~p",
        [?MODULE, Job1#job.id, Job1#job.pid]),
    Job1;

job_runnable(#job{state={error, _Reason}} = Job0) ->
    ets:delete(couch_scheduler_timeout_box, {Job0#job.timeout_until, Job0#job.id}),
    Job1 = Job0#job{state=runnable},
    ets:insert(?MODULE, Job1),
    Job1.

job_error(#job{state=running} = Job0, Reason) ->
    ErroredAt = case Job0#job.errored_at of
        undefined ->
            os:timestamp();
        Other ->
            Other
    end,
    Errors = Job0#job.error_retries+1,
    TimeoutTimestamp = timer_offset(ErroredAt, Errors*?ERROR_RETRY_MILLISECONDS),
    case Job0#job.pid of
        undefined ->
            ok;
        _ ->
            gen_server:cast(Job0#job.pid, stop)
    end,
    Job1 = Job0#job{
        error_retries=Errors,
        errored_at=ErroredAt,
        timeout_until=TimeoutTimestamp,
        state={error, Reason}
    },
    ets:insert(couch_scheduler_timeout_box, {{TimeoutTimestamp, Job1#job.id}}),
    ets:insert(?MODULE, Job1),
    Job1.

job_remove(Job) ->
    #job{
        timeout_until=Timeout,
        stopped_at=StoppedAt,
        started_at=StartedAt,
        id=JobID,
        pid=Pid
    } = Job,
    true = case Job of
        #job{state=running} ->
            ets:delete(couch_scheduler_running, {StartedAt, JobID});
        #job{state=runnable} ->
            ets:delete(couch_scheduler_runnable, {StoppedAt, JobID});
        #job{state={error, _Reason}} ->
            ets:delete(couch_scheduler_timeout_box, {Timeout, JobID})
    end,
    true = ets:delete(?MODULE, JobID),
    case Pid of
        undefined ->
            ok;
        _ ->
            gen_server:cast(Pid, stop),
            supervisor:terminate_child(couch_scheduler_sup, Pid)
    end,
    ok.


-spec running_job_count() -> non_neg_integer().
running_job_count() ->
    ets:info(couch_scheduler_running, size).


-spec runnable_job_count() -> non_neg_integer().
runnable_job_count() ->
    ets:info(couch_scheduler_runnable, size).


-spec job_by_pid(pid()) -> {ok, #job{}} | {error, not_found}.
job_by_pid(Pid) when is_pid(Pid) ->
    case ets:match_object(?MODULE, #job{pid=Pid, _='_'}) of
        [] ->
            {error, not_found};
        [#job{}=Job] ->
            {ok, Job}
    end.

-spec job_by_id(job_id()) -> {ok, #job{}} | {error, not_found}.
job_by_id(Id) ->
    case ets:lookup(?MODULE, Id) of
        [] ->
            {error, not_found};
        [#job{}=Job] ->
            {ok, Job}
    end.


format_timeout(Return) ->
    State = element(tuple_size(Return), Return),
    #state{max_jobs=MaxJobs, churn=Churn, max_churn=MaxChurn} = State,
    {NewChurn, NewTimeout} = case next_rescheduling_timeout() of
        Timeout when Timeout > 0 ->
            {0, Timeout};
        _Timeout ->
            JobsRescheduled = reschedule(MaxJobs, Churn, MaxChurn),
            {JobsRescheduled + Churn, next_rescheduling_timeout()}
    end,
    case Return of
        {Reply, _State} ->
            {Reply, State#state{churn=NewChurn}, NewTimeout};
        {Reply, Message, _State} ->
            {Reply, Message, State, NewTimeout}
    end.


next_rescheduling_timeout() ->
    Now = os:timestamp(),
    {TimeoutBoxTS, _TID} = ets:first(couch_scheduler_timeout_box),
    {RunningJobTS, _RID} = ets:first(couch_scheduler_running),
    Min = lists:min(timer:now_diff(TimeoutBoxTS, Now), timer:now_diff(RunningJobTS, Now)) div 1000,
    max(Min, 0).


-spec reschedule(integer(), integer(), integer()) -> integer().
reschedule(MaxJobs, Churn, MaxChurn) ->
    expire_timeout_box(),
    stop_excess_jobs(MaxJobs),
    JobsStarted = start_runnable_jobs(lists:min([MaxJobs, MaxChurn - Churn, runnable_job_count()])),
    JobsRotated = rotate_old_jobs(lists:min([MaxJobs, MaxChurn - Churn - JobsStarted, runnable_job_count()])),
    JobsStarted + JobsRotated.


expire_timeout_box() ->
    case ets:first(couch_scheduler_timeout_box) of
        '$end_of_table' ->
            ok;
        {Timestamp, JobID} ->
            case timer:now_diff(Timestamp, os:timestamp()) < 0 of
                true ->
                    {ok, Job} = job_by_id(JobID),
                    job_runnable(Job),
                    expire_timeout_box();
                false ->
                    ok
            end
    end.


stop_excess_jobs(MaxJobs) ->
    stop_excess_jobs_int(running_job_count(), MaxJobs).

stop_excess_jobs_int(RunningJobCount, MaxJobs) ->
    case RunningJobCount > MaxJobs of
        true ->
            {_Timestamp, JobID} = ets:first(couch_scheduler_running),
            {ok, Job} = job_by_id(JobID),
            job_runnable(Job),
            stop_excess_jobs_int(RunningJobCount-1, MaxJobs);
        false ->
            ok
    end.

% Rotate at most MaxJobsToRotate
-spec rotate_old_jobs(integer()) -> integer().
rotate_old_jobs(MaxJobsToRotate) ->
    rotate_old_jobs_int(MaxJobsToRotate, 0).

rotate_old_jobs_int(JobsRotated, JobsRotated) ->
    JobsRotated;

rotate_old_jobs_int(MaxJobsToRotate, JobsRotated) ->
    case ets:first(couch_scheduler_running) of
        {Timestamp, JobID} ->
            case timer:now_diff(Timestamp, os:timestamp()) < 0 of
                true ->
                    {ok, Job} = job_by_id(JobID),
                    job_runnable(Job),
                    start_runnable_job(),
                    rotate_old_jobs_int(MaxJobsToRotate, JobsRotated+1);
                false ->
                    JobsRotated
            end;
        '$end_of_table' ->
            JobsRotated
    end.

-spec start_runnable_jobs(integer()) -> integer().
start_runnable_jobs(MaxJobs) ->
    RunningJobCount = running_job_count(),
    case MaxJobs - RunningJobCount of
        JobsToStart when JobsToStart < 0 ->
            % Already too many jobs running
            0;
        JobsToStart ->
            start_runnable_jobs_int(0, JobsToStart)
    end.

start_runnable_jobs_int(JobsStarted, JobsStarted) ->
    JobsStarted;

start_runnable_jobs_int(JobsStarted, JobsToStart) ->
    case start_runnable_job() of
        false ->
            JobsStarted;
        {ok, _Job} ->
            start_runnable_jobs_int(JobsStarted+1, JobsToStart)
    end.

-spec start_runnable_job() -> false | {ok, #job{}}.
start_runnable_job() ->
    case ets:first(couch_scheduler_runnable) of
        '$end_of_table' ->
            false;
        {_Timestamp, JobID} ->
            {ok, Job} = job_by_id(JobID),
            {ok, job_running(Job)}
    end.

timer_offset(Start, Offset) ->
    {Megas, Secs, Micros} = Start,
    {Megas, Secs+(Offset div 1000), Micros+(Offset rem 1000)*1000}.
