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

-module(couch_scheduler_sup).
-behaviour(supervisor).
-vsn(1).

-export([start_link/0, init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init(_Args) ->
    Start = {couch_scheduler_job, start_link, []},
    Restart = temporary, % A crashed job is not entitled to immediate restart.
    Shutdown = 5000,
    Type = worker,
    Modules = [couch_scheduler_job],

    RestartStrategy = simple_one_for_one,
    MaxR = 10,
    MaxT = 3,

    ChildSpec =
        {undefined, Start, Restart, Shutdown, Type, Modules},
    {ok, {{RestartStrategy, MaxR, MaxT}, [ChildSpec]}}.
