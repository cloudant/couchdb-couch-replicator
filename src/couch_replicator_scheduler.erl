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

%% types
-type event_type() :: started | stopped | crashed.
-type event() :: {Type:: event_type(), When :: erlang:timestamp()}.
-type history() :: [Events :: event()].

%% definitions
-define(MAX_HISTORY, 20).
-define(MINIMUM_CRASH_INTERVAL, 60 * 1000000).

-define(DEFAULT_MAX_JOBS, 100).
-define(DEFAULT_MAX_CHURN, 20).
-define(DEFAULT_SCHEDULER_INTERVAL, 60000).
-define(ERROR_RETRY_MILLISECONDS, 30000).
-record(state, {interval, max_jobs, max_churn}).

-record(job, {
    id :: job_id(),
    rep :: #rep{},
    pid :: pid(),
    history :: history(),
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
        history = []},
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
    {ok, #state{interval = Interval, max_jobs = MaxJobs, max_churn = MaxChurn}}.


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
        {ok, #job{}=Job0} ->
            case Reason of
                normal ->
                    job_runnable(Job0);
                {error, EReason} ->
                    Job1 = update_history(Job0, crashed, os:timestamp()),
                    true = ets:insert(?MODULE, Job1),
                    job_error(Job1, EReason)
            end,
            couch_log:notice("~p: Job ~p died with reason: ~p",
                             [?MODULE, Job0#job.id, Reason]),
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
            NewJob = Job#job{
                pid = Child,
                errored_at = undefined,
                error_retries = 0,
                started_at = StartedAt,
                state=running
            },
            true = ets:insert(?MODULE, NewJob),
            true = ets:insert(couch_scheduler_running, {{StartedAt, Job#job.id}}),
            true = ets:delete(couch_scheduler_runnable, {Job#job.stopped_at, Job#job.id}),
            couch_log:notice("~p: Job ~p started as ~p",
                [?MODULE, NewJob#job.id, NewJob#job.pid]),
            NewJob;
        {error, Reason} ->
            couch_log:notice("~p: Job ~p failed to start for reason ~p",
                [?MODULE, Job, Reason]),
            job_error(Job, Reason)
    end.



job_runnable(#job{state=running} = Job0) ->
    StoppedAt = os:timestamp(),
    gen_server:cast(Job0#job.pid, stop),
    supervisor:terminate_child(couch_scheduler_sup, Job0#job.pid),
    Job1 = Job0#job{pid = undefined, stopped_at = StoppedAt, state=runnable},
    Job2 = update_history(Job1, stopped, os:timestamp()),
    ets:insert(?MODULE, Job2),
    true = ets:insert(?MODULE, Job2),
    true = ets:delete(couch_scheduler_running, {Job2#job.started_at, Job2#job.id}),
    true = ets:insert(couch_scheduler_runnable, {{StoppedAt, Job2#job.id}}),
    couch_log:notice("~p: Job ~p stopped as ~p",
        [?MODULE, Job2#job.id, Job2#job.pid]),
    Job2;

job_runnable(#job{state={error, _Reason}} = Job) ->
    ets:delete(couch_scheduler_timeout_box, {Job#job.timeout_until, Job#job.id}),
    NewJob = Job#job{state=runnable},
    ets:insert(?MODULE, NewJob),
    NewJob.

job_error(#job{state=running} = Job, Reason) ->
    ErroredAt = case Job#job.errored_at of
        undefined ->
            os:timestamp();
        Other ->
            Other
    end,
    Errors = Job#job.error_retries+1,
    TimeoutTimestamp = timer_offset(ErroredAt, Errors*?ERROR_RETRY_MILLISECONDS),
    case Job#job.pid of
        undefined ->
            ok;
        _ ->
            gen_server:cast(Job#job.pid, stop)
    end,
    NewJob = Job#job{
        error_retries=Errors,
        errored_at=ErroredAt,
        timeout_until=TimeoutTimestamp,
        state={error, Reason}
    },
    ets:insert(couch_scheduler_timeout_box, {{TimeoutTimestamp, Job#job.id}}),
    ets:insert(?MODULE, NewJob),
    NewJob.

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


-spec update_history(#job{}, event_type(), erlang:timestamp()) -> #job{}.
update_history(Job, Type, When) ->
    History0 = [{Type, When} | Job#job.history],
    History1 = lists:sublist(History0, ?MAX_HISTORY),
    Job#job{history = History1}.


format_timeout(Return) ->
    State = element(tuple_size(Return), Return),
    Timeout = case next_rescheduling_timeout() of
        Timeout0 when Timeout0 > 0 ->
            Timeout0;
        _Timeout0 ->
            reschedule(State#state.max_jobs),
            next_rescheduling_timeout()
    end,
    case Return of
        {Reply, _State} ->
            {Reply, State, Timeout};
        {Reply, Message, _State} ->
            {Reply, Message, State, Timeout}
    end.


next_rescheduling_timeout() ->
    Now = os:timestamp(),
    {TimeoutBoxTS, _TID} = ets:first(couch_scheduler_timeout_box),
    {RunningJobTS, _RID} = ets:first(couch_scheduler_running),
    Min = min(timer:now_diff(TimeoutBoxTS, Now), timer:now_diff(RunningJobTS, Now)) div 1000,
    max(Min, 0).


reschedule(MaxJobs) ->
    expire_timeout_box(),
    stop_excess_jobs(MaxJobs),
    stop_old_jobs(),
    start_runnable_jobs(MaxJobs).


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

stop_old_jobs() ->
    RunnableJobCount = runnable_job_count(),
    stop_old_jobs_int(RunnableJobCount, 0).

stop_old_jobs_int(JobsStopped, JobsStopped) ->
    ok;

stop_old_jobs_int(RunnableJobCount, JobsStopped) ->
    case ets:first(couch_scheduler_running) of
        {Timestamp, JobID} ->
            case timer:now_diff(Timestamp, os:timestamp()) < 0 of
                true ->
                    {ok, Job} = job_by_id(JobID),
                    job_runnable(Job),
                    start_runnable_job(),
                    stop_old_jobs_int(RunnableJobCount, JobsStopped+1);
                false ->
                    ok
            end;
        '$end_of_table' ->
            ok
    end.

start_runnable_jobs(MaxJobs) ->
    RunningJobCount = running_job_count(),
    start_runnable_jobs_int(RunningJobCount, MaxJobs).


start_runnable_jobs_int(RunningJobCount, MaxJobs) when RunningJobCount >= MaxJobs ->
    ok;

start_runnable_jobs_int(RunningJobCount, MaxJobs) ->
    case start_runnable_job() of
        false ->
            ok;
        {ok, _Job} ->
            start_runnable_jobs_int(RunningJobCount+1, MaxJobs)
    end.

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
