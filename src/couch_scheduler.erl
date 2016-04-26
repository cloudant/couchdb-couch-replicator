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
-export([start_link/0, add_job/3, remove_job/1]).

%% gen_server callbacks
-export([init/1, terminate/2, code_change/3]).
-export([handle_call/3, handle_cast/2, handle_info/2]).
-export([format_status/2]).

%% config_listener callback
-export([handle_config_change/5, handle_config_terminate/3]).

%% definitions
-define(DEFAULT_MAX_JOBS, 100).
-define(DEFAULT_SCHEDULER_INTERVAL, 5000).
-define(MAX_HISTORY, 100).
-record(state, {interval, timer, max_jobs}).
-record(job, {
          module :: module(),
          id :: job_id(),
          args :: job_args(),
          pid :: pid(),
          history :: [erlang:timestamp()]}).

%% public functions

-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


-spec add_job(module(), job_id(), job_args()) -> ok | {error, already_added}.
add_job(Module, Id, Args) when is_atom(Module), Id /= undefined ->
    Job = #job{module = Module,
        id = Id,
        args = Args,
        history = []},
    gen_server:call(?MODULE, {add_job, Job}).


-spec remove_job(job_id()) -> ok.
remove_job(Id) ->
    gen_server:call(?MODULE, {remove_job, Id}).


%% gen_server functions

init(_) ->
    ?MODULE = ets:new(?MODULE, [named_table, {keypos, #job.id}]),
    ok = config:listen_for_changes(?MODULE, self()),
    Interval = config:get_integer("replicator", "interval", ?DEFAULT_SCHEDULER_INTERVAL),
    MaxJobs = config:get_integer("replicator", "max_jobs", ?DEFAULT_MAX_JOBS),
    {ok, Timer} = timer:send_after(Interval, reschedule),
    {ok, #state{interval = Interval, max_jobs = MaxJobs, timer = Timer}}.


handle_call({add_job, Job}, _From, State) ->
    case add_job_int(Job) of
        true ->
            {reply, ok, State};
        false ->
            {reply, {error, already_added}, State}
    end;

handle_call({remove_job, Id}, _From, State) ->
    case job_by_id(Id) of
        {ok, Job} ->
            ok = stop_job_int(Job),
            true = remove_job_int(Job),
            {reply, ok, State};
        undefined ->
            {reply, ok, State}
    end;

handle_call(_, _From, State) ->
    {noreply, State}.


handle_cast({set_max_jobs, MaxJobs}, State) when is_integer(MaxJobs), MaxJobs > 0 ->
    couch_log:notice("~p: max_jobs set to ~B", [?MODULE, MaxJobs]),
    {noreply, State#state{max_jobs = MaxJobs}};

handle_cast({set_interval, Interval}, State) when is_integer(Interval), Interval > 0 ->
    couch_log:notice("~p: interval set to ~B", [?MODULE, Interval]),
    {noreply, State#state{interval = Interval}};

handle_cast(_, State) ->
    {noreply, State}.


handle_info(reschedule, State0) ->
    State1 = reschedule(State0),
    {ok, cancel} = timer:cancel(State1#state.timer),
    {ok, Timer} = timer:send_after(State1#state.interval, reschedule),
    {noreply, State1#state{timer = Timer}};

handle_info({'DOWN', _Ref, process, Pid, Reason}, State) ->
    case job_by_pid(Pid) of
        {ok, #job{}=Job0} ->
            couch_log:notice("~p: Job ~p died with reason: ~p",
                             [?MODULE, Job0#job.id, Reason]),
            Job1 = Job0#job{
                     pid = undefined,
                     history = update_history(Job0#job.history)
                    },
            true = ets:insert(?MODULE, Job1),
            {noreply, State};
        {error, not_found} ->
            % removed in remove_job and should not be reinserted.
            {noreply, State}
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
     {pending_jobs, pending_job_count()}].


%% config listener functions

handle_config_change("replicator", "max_jobs", V, _, Pid) ->
    ok = gen_server:cast(Pid, {set_max_jobs, list_to_integer(V)}),
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

start_jobs(Count) ->
    Runnable0 = pending_jobs(),
    Runnable1 = lists:sort(fun oldest_job_first/2, Runnable0),
    Runnable2 = lists:sublist(Runnable1, Count),
    lists:foreach(fun start_job_int/1, Runnable2).


stop_jobs(Count) ->
    Running0 = running_jobs(),
    Running1 = lists:sort(fun oldest_job_first/2, Running0),
    Running2 = lists:sublist(Running1, Count),
    lists:foreach(fun stop_job_int/1, Running2).


oldest_job_first(#job{} = A, #job{} = B) ->
    last_run(A) =< last_run(B).


-spec last_run(#job{}) -> erlang:timestamp() | never.
last_run(#job{}=Job) ->
    case Job#job.history of
        [] ->
            never;
        [LastRun | _] ->
            LastRun
    end.


-spec add_job_int(#job{}) -> boolean().
add_job_int(#job{} = Job) ->
    ets:insert_new(?MODULE, Job).


start_job_int(#job{pid = Pid}) when Pid /= undefined ->
    ok;

start_job_int(#job{} = Job0) ->
    Args = [Job0#job.module, Job0#job.id, Job0#job.args],
    case supervisor:start_child(couch_scheduler_sup, Args) of
        {ok, Child} ->
            monitor(process, Child),
            started = gen_server:call(Child, start),
            Job1 = Job0#job{pid = Child},
            true = ets:insert(?MODULE, Job1),
            couch_log:notice("~p: Job ~p started as ~p",
                [?MODULE, Job1#job.id, Job1#job.pid]);
        {error, Reason} ->
            couch_log:notice("~p: Job ~p failed to start for reason ~p",
                [?MODULE, Job0, Reason])
    end.


-spec stop_job_int(#job{}) -> ok | {error, term()}.
stop_job_int(#job{pid = undefined}) ->
    ok;

stop_job_int(#job{} = Job0) ->
    stopped = gen_server:call(Job0#job.pid, stop),
    Job1 = Job0#job{pid = undefined},
    true = ets:insert(?MODULE, Job1),
    couch_log:notice("~p: Job ~p stopped as ~p",
        [?MODULE, Job0#job.id, Job0#job.pid]),
    supervisor:terminate_child(couch_scheduler_sup, Job0#job.pid).


-spec remove_job_int(#job{}) -> true.
remove_job_int(#job{} = Job) ->
    ets:delete(?MODULE, Job#job.id).


-spec running_job_count() -> non_neg_integer().
running_job_count() ->
    ets:info(?MODULE, size) - pending_job_count().


-spec running_jobs() -> [#job{}].
running_jobs() ->
    ets:tab2list(?MODULE) -- pending_jobs().


-spec pending_job_count() -> non_neg_integer().
pending_job_count() ->
    ets:select_count(?MODULE, #job{pid=undefined, _='_'}).


-spec pending_jobs() -> [#job{}].
pending_jobs() ->
    ets:match_object(?MODULE, #job{pid=undefined, _='_'}).


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

-spec update_history([erlang:timestamp()]) -> [erlang:timestamp()].
update_history(History0) when is_list(History0) ->
    History1 = [os:timestamp() | History0],
    lists:sublist(History1, ?MAX_HISTORY).


-spec reschedule(#state{}) -> #state{}.
reschedule(State) ->
    Max = State#state.max_jobs,
    Running = running_job_count(),
    Pending = pending_job_count(),
    if
        Running == Max ->
            ok;
        Running > Max ->
            stop_jobs(Running - Max);
        Running < Max ->
            start_jobs(Max - Running)
    end,
    State.

