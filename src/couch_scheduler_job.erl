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
-export([start_link/1]).

%% gen_server callbacks
-export([init/1, terminate/2, code_change/3]).
-export([handle_call/3, handle_cast/2, handle_info/2]).

-callback init(Args :: term()) ->
    {ok, Id :: term(), State :: term()}.

-callback start(State :: term()) ->
    {ok, NewState :: term()}.

-callback stop(State :: term()) ->
    {ok, NewState :: term()}.


start_link(#scheduled_job{} = Job) ->
    gen_server:start_link(
      {global, {?MODULE, Job#scheduled_job.id}},
      ?MODULE,
      Job#scheduled_job.jobspec,
      []).


init(_) ->
    {ok, nil}.


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
