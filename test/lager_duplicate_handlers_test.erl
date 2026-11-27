%% Tests for GM-343493: configuring a sink twice must leave one set of
%% handlers, and duplicated handlers must not multiply log rotation timers
%% or leave a sink stuck in sync mode.
-module(lager_duplicate_handlers_test).

-include_lib("eunit/include/eunit.hrl").

-define(SINK, dup_test_sink).
-define(TIMER_SINK, dup_test_timer_sink).

all_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      fun configure_twice_leaves_one_set/1,
      fun empty_sink_gets_handlers_back/1,
      fun rotation_timers_stay_constant_with_duplicate/1,
      fun legacy_rotate_does_not_arm_extra_timer/1,
      fun manual_rotate_does_not_arm_extra_timer/1,
      fun throttle_returns_to_async_after_drain/1
     ]}.

setup() ->
    {ok, TestDir} = lager_util:create_test_dir(),
    ok = error_logger:tty(false),
    ok = lager_util:safe_application_load(lager),
    ok = application:set_env(lager, handlers, []),
    ok = application:set_env(lager, error_logger_redirect, false),
    ok = application:set_env(lager, async_threshold, undefined),
    ok = application:set_env(lager, crash_log, false),
    ok = application:set_env(lager, drop_if_sync, true),
    ok = application:set_env(lager, extra_sinks, [{?SINK, sink_def(TestDir)}]),
    %% in TEST builds lager_backend_throttle records its mode toggles in this
    %% table (the ?TOGGLE_SYNC/?TOGGLE_ASYNC macros); without it the handler
    %% crashes on the first toggle
    _ = catch ets:delete(async_threshold_test),
    _ = ets:new(async_threshold_test, [set, named_table, public]),
    _ = ets:insert_new(async_threshold_test, {sync_toggled, 0}),
    _ = ets:insert_new(async_threshold_test, {async_toggled, 0}),
    ok = lager:start(),
    ok = timer:sleep(5),
    TestDir.

cleanup(TestDir) ->
    _ = stop_timer_trace(),
    ok = application:stop(lager),
    ok = application:stop(goldrush),
    ok = error_logger:tty(true),
    _ = catch ets:delete(async_threshold_test),
    %% leave the lager environment as we found it; later suites start lager
    %% with their own expectations
    _ = application:unset_env(lager, handlers),
    _ = application:unset_env(lager, extra_sinks),
    _ = application:unset_env(lager, error_logger_redirect),
    _ = application:unset_env(lager, async_threshold),
    _ = application:unset_env(lager, crash_log),
    _ = application:unset_env(lager, drop_if_sync),
    ok = lager_util:delete_test_dir(),
    _ = TestDir,
    ok.

%% 1. Configuring the sink a second time leaves one set of handlers.
configure_twice_leaves_one_set(TestDir) ->
    fun() ->
        ok = reset_file_check(),
        Before = gen_event:which_handlers(?SINK),
        %% console + 4 file backends + throttle, no duplicates
        ?assertEqual(6, length(Before)),
        ?assertEqual(6, length(lists:usort(Before))),
        ok = lager_app:boot(?SINK),
        After = gen_event:which_handlers(?SINK),
        ?assertEqual(lists:usort(Before), lists:usort(After)),
        ?assertEqual(length(After), length(lists:usort(After)))
    end.

%% 2. A sink that exists without any handlers gets the full set back.
empty_sink_gets_handlers_back(TestDir) ->
    fun() ->
        ok = reset_file_check(),
        SinkName = new_sink(?TIMER_SINK),
        ?assertEqual([], gen_event:which_handlers(SinkName)),
        ok = lager_app:configure_sink(SinkName, sink_def(TestDir)),
        Handlers = gen_event:which_handlers(SinkName),
        ?assertEqual(6, length(Handlers)),
        ?assertEqual(6, length(lists:usort(Handlers)))
    end.

%% 3. With a duplicate forced in the sink, the number of pending rotation
%%    timers stays constant across simulated midnights.
rotation_timers_stay_constant_with_duplicate(TestDir) ->
    {timeout, 60, fun() ->
        ok = reset_file_check(),
        ok = start_timer_trace(),
        SinkName = new_sink(?TIMER_SINK),
        ok = lager_app:configure_sink(SinkName, sink_def(TestDir)),
        Sink = whereis(SinkName),
        %% the four file backends each arm exactly one rotation timer
        Pairs0 = collect_timer_pairs(Sink),
        ?assertEqual(4, length(Pairs0)),
        %% force a duplicate of the info.log handler, as a second boot of the
        %% sink used to
        {Id, Config} = file_handler(TestDir, info, "info.log"),
        ok = gen_event:add_handler(SinkName, Id, Config),
        DupPairs = collect_timer_pairs(Sink),
        ?assertEqual(1, length(DupPairs)),
        All = Pairs0 ++ DupPairs,
        %% console + 4 file backends + throttle + the forced duplicate
        ?assertEqual(7, length(gen_event:which_handlers(SinkName))),
        ok = simulate_midnights(Sink, All, 6),
        ok = stop_timer_trace()
    end}.

%% 4. A legacy {rotate, File} message (a timer armed by an older version of
%%    the module) must not arm an extra timer while one is pending.
legacy_rotate_does_not_arm_extra_timer(TestDir) ->
    fun() ->
        ok = reset_file_check(),
        ok = start_timer_trace(),
        SinkName = new_sink(?TIMER_SINK),
        ok = lager_app:configure_sink(SinkName, sink_def(TestDir)),
        Sink = whereis(SinkName),
        Pairs0 = collect_timer_pairs(Sink),
        ?assertEqual(4, length(Pairs0)),
        %% one legacy message per file, as the pre-upgrade timers would send
        [Sink ! Msg || {_Ref, Msg} <- Pairs0],
        _ = gen_event:which_handlers(Sink),
        ?assertEqual([], collect_timer_pairs(Sink)),
        %% but a rotation did happen
        ?assert(filelib:is_regular(filename:join(TestDir, "info.log.0"))),
        ok = stop_timer_trace()
    end.

%% 5. A manual rotate must not arm an extra timer.
manual_rotate_does_not_arm_extra_timer(TestDir) ->
    fun() ->
        ok = reset_file_check(),
        ok = start_timer_trace(),
        SinkName = new_sink(?TIMER_SINK),
        ok = lager_app:configure_sink(SinkName, sink_def(TestDir)),
        Sink = whereis(SinkName),
        Pairs0 = collect_timer_pairs(Sink),
        ?assertEqual(4, length(Pairs0)),
        {Id, _} = file_handler(TestDir, info, "info.log"),
        ok = gen_event:call(SinkName, Id, rotate, 5000),
        ?assertEqual([], collect_timer_pairs(Sink)),
        ?assert(filelib:is_regular(filename:join(TestDir, "info.log.0"))),
        ok = stop_timer_trace()
    end.

%% 6. With drop_if_sync = true, a sink that flipped to sync mode because of a
%%    non-log backlog returns to async once the backlog has drained, and
%%    logs again.
throttle_returns_to_async_after_drain(TestDir) ->
    {timeout, 60, fun() ->
        ?assertEqual(true, lager_config:get({?SINK, async})),
        %% queue a log event behind a non-log backlog bigger than the async
        %% threshold; when the sink resumes, the log event is processed while
        %% the queue is over the threshold, which flips it into sync mode
        ok = sys:suspend(?SINK),
        ok = lager:log(?SINK, info, self(), "head", []),
        _ = [?SINK ! {rotate, "no-such-file"} || _ <- lists:seq(1, 800)],
        ok = sys:resume(?SINK),
        ?assertEqual(ok, wait_until(fun() -> lager_config:get({?SINK, async}) =:= false end, 5000)),
        %% the non-log backlog drains; the periodic self-check flips the sink
        %% back to async without needing another log event
        ?assertEqual(ok, wait_until(fun() -> lager_config:get({?SINK, async}) =:= true end, 5000)),
        %% and new lines are written again
        ok = lager:log(?SINK, info, self(), "after-recovery", []),
        LogFile = filename:join(TestDir, "info.log"),
        ?assertEqual(ok, wait_until(fun() -> file_contains(LogFile, <<"after-recovery">>) end, 5000)),
        ok
    end}.

%% -- helpers ---------------------------------------------------------------

new_sink(Name) ->
    ChildId = lager_util:make_internal_sink_name(Name),
    {ok, _} = supervisor:start_child(lager_sup,
                                     {ChildId,
                                      {gen_event, start_link, [{local, Name}]},
                                      permanent, 5000, worker, dynamic}),
    Name.

sink_def(TestDir) ->
    [{handlers,
      [{lager_console_backend, [{level, none}]} |
       [{lager_file_backend, file_config(TestDir, Level, Name)}
        || {Level, Name} <- [ {info, "info.log"},
                              {warning, "warning.log"},
                              {error, "error.log"},
                              {debug, "debug.log"} ]]]},
     {async_threshold, 500},
     {async_threshold_window, 50}].

file_handler(TestDir, Level, Name) ->
    File = filename:join(TestDir, Name),
    {{lager_file_backend, File}, file_config(TestDir, Level, Name)}.

file_config(TestDir, Level, Name) ->
    File = filename:join(TestDir, Name),
    [{level, Level}, {file, File}, {date, "$D0"}, {count, 10}].

%% lager_app keeps the "same file in multiple backends" check in the calling
%% process's dictionary; eunit may reuse a process across tests, so start
%% each test with a clean check.
reset_file_check() ->
    _ = erase('__lager_file_backend_filenames'),
    ok.

%% Count the rotation timers a sink arms by tracing erlang:start_timer/3.
%% Every rotation timer is armed as erlang:start_timer(_, self(), {rotate, _})
%% from the sink process, so the call trace carries the message and the
%% return trace carries the timer reference.
start_timer_trace() ->
    erlang:trace_pattern({erlang, start_timer, 3}, [{'_', [], [{return_trace}]}], []),
    erlang:trace(new, true, [call]),
    ok.

stop_timer_trace() ->
    erlang:trace(new, false, [call]),
    erlang:trace_pattern({erlang, start_timer, 3}, false, []),
    ok.

collect_timer_pairs(Sink) ->
    collect_timer_pairs(Sink, undefined, []).

collect_timer_pairs(Sink, PendingMsg, Acc) ->
    receive
        {trace, Sink, call, {erlang, start_timer, [_Time, _Dest, Msg]}} ->
            collect_timer_pairs(Sink, Msg, Acc);
        {trace, Sink, return_from, {erlang, start_timer, 3}, Ref} ->
            collect_timer_pairs(Sink, undefined, [{Ref, PendingMsg} | Acc]);
        {trace, _Other, _Type, _Info} ->
            collect_timer_pairs(Sink, PendingMsg, Acc)
    after 500 ->
        lists:reverse(Acc)
    end.

%% Deliver every pending rotation timer once, as a midnight would, and check
%% that the same number of timers is armed again.
simulate_midnights(_Sink, _Pairs, 0) ->
    ok;
simulate_midnights(Sink, Pairs, N) ->
    [begin
         _ = erlang:cancel_timer(Ref),
         Sink ! {timeout, Ref, Msg}
     end || {Ref, Msg} <- Pairs],
    _ = gen_event:which_handlers(Sink),
    NewPairs = collect_timer_pairs(Sink),
    ?assertEqual(length(Pairs), length(NewPairs)),
    simulate_midnights(Sink, NewPairs, N - 1).

wait_until(Fun, Timeout) ->
    wait_until(Fun, Timeout, 50).

wait_until(Fun, Timeout, Step) when Timeout =< 0 ->
    case Fun() of
        true -> ok;
        false -> timeout
    end;
wait_until(Fun, Timeout, Step) ->
    case Fun() of
        true -> ok;
        false ->
            timer:sleep(Step),
            wait_until(Fun, Timeout - Step, Step)
    end.

file_contains(File, Bin) ->
    case file:read_file(File) of
        {ok, Content} -> binary:match(Content, Bin) =/= nomatch;
        _ -> false
    end.
