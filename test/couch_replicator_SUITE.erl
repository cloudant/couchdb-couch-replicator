-module(couch_replicator_SUITE).
-include_lib("common_test/include/ct.hrl").

-include("couch_replicator.hrl").

-compile(export_all).

%%--------------------------------------------------------------------
%% Common Test interface functions -----------------------------------
%%--------------------------------------------------------------------

all() ->
    [cannot_schedule_identical_jobs,
     can_schedule_non_identical_jobs,
     test_add_remove_add_same_job].

init_per_suite(Config) ->
    DbDirs = setup_db_directories(?config(priv_dir, Config)),
    lists:append(Config, DbDirs).

init_per_testcase(_Name, Config) ->
    [DbEtcDir, DbDataDir, DbLogDir] = 
        [proplists:get_value(Key, Config) || Key <- [db_etc_dir, db_data_dir, db_log_dir]],
    {ini_files, DefaultIniPath, LocalIniPath} = 
        write_db_ini_files(DbEtcDir, DbDataDir, DbLogDir),

    application:set_env(config, ini_files, [DefaultIniPath, LocalIniPath]),
    
    {ok, Apps} = application:ensure_all_started(couch_replicator),
    ct:pal("Applications started: ~p~n", [Apps]),
    Config.

end_per_testcase(_Name, _Config) ->
    application:stop(couch_replicator),
    ok.


%%--------------------------------------------------------------------
%% Test Cases --------------------------------------------------------
%%--------------------------------------------------------------------

cannot_schedule_identical_jobs(_Config) ->
    Rep = #rep{id=dummy_repl_id},
    ok = couch_replicator_scheduler:add_job(Rep),
    {error, already_added} = couch_replicator_scheduler:add_job(Rep),
    ok.

can_schedule_non_identical_jobs(_Config) ->
    Rep1 = #rep{id=dummy_repl_id_1},
    Rep2 = #rep{id=dummy_repl_id_2},
    ok = couch_replicator_scheduler:add_job(Rep1),
    ok = couch_replicator_scheduler:add_job(Rep2),
    ok.    

test_add_remove_add_same_job(_Config) ->
    Rep = #rep{id=dummy_repl_id},
    ok = couch_replicator_scheduler:add_job(Rep),
    ok = couch_replicator_scheduler:remove_job(Rep#rep.id),
    ok = couch_replicator_scheduler:add_job(Rep),
    ok.


%%--------------------------------------------------------------------
%% Internal functions ------------------------------------------------
%%--------------------------------------------------------------------

% Creates directories for database data/logs, etc in the private area
% provided by the CT framework
setup_db_directories(PrivDir) ->
    Dirs = [DbEtcDir, DbDataDir, DbLogDir] = [PrivDir ++ SubDir || SubDir <- ["etc/", "data/", "logs/"]],
    [ok, ok, ok] = [file:make_dir(Dir) || Dir <- Dirs],
    [{db_etc_dir, DbEtcDir}, {db_data_dir, DbDataDir}, {db_log_dir, DbLogDir}].

% Creates database ini files that are enough to get the tests to run
write_db_ini_files(EtcDir, DataDir, LogDir) ->
    DefaultIniPath = EtcDir ++ "default.ini",
    DefaultIniContent = 
        "[couchdb]\n"
        "uuid = fake_uuid_for_dev\n"
        "database_dir = " ++ DataDir ++ "\n"
        "view_index_dir = " ++ DataDir ++ "\n"
        "file = " ++ LogDir ++ "node.log\n"
        "\n"
        "[log]\n"
        "backend = stderr\n"
        "level = info\n",
    ok = file:write_file(DefaultIniPath, DefaultIniContent),

    LocalIniPath = EtcDir ++ "local.ini",
    LocalIniContent = "",  % Presuming there will be content at some point.
    ok = file:write_file(LocalIniPath, LocalIniContent),
    {ini_files, DefaultIniPath, LocalIniPath}.
