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

-module(couch_replicator_job).
-behaviour(couch_scheduler_job).

-include("couch_replicator.hrl").

%% couch_scheduler_job api
-export([init/2, start/3, stop/3]).

%% couch_scheduler_job functions

init(Id, _) ->
    {ok, undefined}.


start(Id, Rep, undefined) ->
    {ok, spawn_link(couch_replicator, replicate, [Rep])}.


stop(Id, _Rep, Pid) ->
    erlang:unlink(Pid),
    exit(Pid, shutdown),
    {ok, undefined}.
