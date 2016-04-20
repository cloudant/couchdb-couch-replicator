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

-module(couch_scheduler_job).
-behaviour(gen_server).
-vsn(1).

-include("couch_scheduler.hrl").

%% public api
-export([start_link/3]).

%% gen_server callbacks
-export([init/1, terminate/2, code_change/3]).
-export([handle_call/3, handle_cast/2, handle_info/2]).

-callback init(Id :: job_id(), Args :: job_args()) ->
    {ok, State :: term()}.

-callback start(Id :: job_id(), Args :: job_args(), State :: term()) ->
    {ok, NewState :: term()}.

-callback stop(Id :: job_id(), Args :: job_args(), State :: term()) ->
    {ok, NewState :: term()}.

%% definitions
-record(state, {module, id, args, job_state}).

start_link(Module, Id, Args) ->
    gen_server:start_link(
      {global, {Module, Id}},
      ?MODULE,
      {Module, Id, Args},
      []).


init({Module, Id, Args}) ->
    {ok, JobState} = Module:init(Id, Args),
    State = #state{
        module = Module,
        id = Id,
        args = Args,
        job_state = JobState
    },
    {ok, State}.


handle_call(start, _From, State0) ->
    #state{
        module = Module,
        id = Id,
        args = Args,
        job_state = JobState0} = State0,
    {ok, JobState1} = Module:start(Id, Args, JobState0),
    State1 = State0#state{job_state=JobState1},
    {reply, started, State1};

handle_call(stop, _From, State0) ->
    #state{
        module = Module,
        id = Id,
        args = Args,
        job_state = JobState0} = State0,
    {ok, JobState1} = Module:stop(Id, Args, JobState0),
    State1 = State0#state{job_state=JobState1},
    {reply, stopped, State1};

handle_call(_Msg, _From, State) ->
    {noreply, State}.


handle_cast(_Msg, State) ->
    {noreply, State}.


handle_info(_Msg, State) ->
    {noreply, State}.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


terminate(_Reason, _State) ->
    ok.
