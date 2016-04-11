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

-module(couch_scheduler).
-behaviour(gen_server).
-behaviour(config_listener).
-vsn(1).

-include("couch_scheduler.hrl").

%% public api
-export([start_link/0, add_job/1, remove_job/1, job_count/0]).

%% gen_server callbacks
-export([init/1, terminate/2, code_change/3]).
-export([handle_call/3, handle_cast/2, handle_info/2]).

%% config_listener callback
-export([handle_config_change/5, handle_config_terminate/3]).

%% definitions
-define(DEFAULT_MAX_JOBS, 100).
-define(SCHEDULER_INTERVAL, 5000).
-record(state, {timer, max_jobs}).

%% public functions

-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


-spec add_job(#jobspec{}) -> ok | {error, already_added}.
add_job(#jobspec{id = Id} = JobSpec) when Id /= undefined ->
    gen_server:call(?MODULE, {add_job, JobSpec}).


-spec remove_job(job_id()) -> ok.
remove_job(JobId) ->
    gen_server:call(?MODULE, {remove_job, JobId}).


-spec job_count() -> non_neg_integer().
job_count() ->
    ets:info(?MODULE, size).


%% gen_server functions

init(_) ->
    ?MODULE = ets:new(?MODULE, [named_table, {keypos, #scheduled_job.id}]),
    ok = config:listen_for_changes(?MODULE, self()),
    MaxJobs = config:get_integer("replicator", "max_jobs", ?DEFAULT_MAX_JOBS),
    {ok, Timer} = timer:send_after(?SCHEDULER_INTERVAL, reschedule),
    {ok, #state{max_jobs = MaxJobs, timer = Timer}}.


handle_call({add_job, JobSpec}, _From, State) ->
    ScheduledJob = #scheduled_job{
        id = JobSpec#jobspec.id,
        jobspec = JobSpec,
        last_run = {0, 0, 0},
        state = runnable},
    case ets:insert_new(?MODULE, ScheduledJob) of
        true ->
            {reply, ok, State};
        false ->
            {reply, {error, already_added}, State}
    end;

handle_call({remove_job, JobId}, _From, State) ->
    true = ets:match_delete(?MODULE, #scheduled_job{id = JobId, _='_'}),
    case global:whereis_name({couch_scheduler_job, JobId}) of
        undefined ->
            {reply, ok, State};
        Pid ->
            supervisor:terminate_child(couch_scheduler_sup, Pid),
            {reply, ok, State}
    end;

handle_call(_, _From, State) ->
    {noreply, State}.


handle_cast({set_max_jobs, MaxJobs}, State) when is_integer(MaxJobs) ->
    {noreply, State#state{max_jobs = MaxJobs}};

handle_cast(_, State) ->
    {noreply, State}.


handle_info(reschedule, State) ->
    Counts = supervisor:count_children(couch_scheduler_sup),
    WorkerCount = proplists:get_value(workers, Counts),
    AvailableSlots = State#state.max_jobs - WorkerCount,
    case AvailableSlots > 0 of
        true ->
            start_jobs(AvailableSlots);
        false ->
            ok
    end,
    {ok, cancel} = timer:cancel(State#state.timer),
    {ok, Timer} = timer:send_after(?SCHEDULER_INTERVAL, reschedule),
    {noreply, State#state{timer = Timer}};

handle_info(_, State) ->
    {noreply, State}.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


terminate(_Reason, _State) ->
    ok.

%% config listener functions

handle_config_change("replicator", "max_jobs", V, _, Pid) ->
    ok = gen_server:cast(Pid, {set_max_jobs, list_to_integer(V)}),
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

start_jobs(Count) ->
    Runnable0 = ets:match_object(?MODULE, #scheduled_job{state = runnable, _='_'}),
    Runnable1 = lists:sort(fun oldest_first/2, Runnable0),
    Runnable2 = lists:sublist(Runnable1, Count),
    lists:foreach(fun start_job/1, Runnable2).


oldest_first(#scheduled_job{} = A, #scheduled_job{} = B) ->
    A#scheduled_job.last_run =< B#scheduled_job.last_run.


start_job(#scheduled_job{} = Job) ->
    case supervisor:start_child(couch_scheduler_sup, [Job]) of
        {ok, Child} ->
            couch_log:notice("Job ~p started as ~p", [Job, Child]),
            true = ets:insert(?MODULE, Job#scheduled_job{state = running});
        {error, Reason} ->
            couch_log:notice("Job ~p failed to start for reason ~p",
                             [Job, Reason])
    end.
