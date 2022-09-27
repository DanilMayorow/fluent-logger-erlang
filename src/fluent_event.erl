-module(fluent_event).

-behaviour(gen_event).

-include("fluent.hrl").

%% API
-export([add_handler/3]).

%% gen_event callbacks
-export([
        init/1,
        handle_event/2,
        handle_call/2,
        handle_info/2,
        terminate/2,
        code_change/3
]).

-spec add_handler(atom(), inet:host(), inet:port_number()) -> ok | {'EXIT', term()} | term().
add_handler(Tag,Host,Port) ->
    gen_event:add_handler(?SERVER, ?MODULE, {Tag,Host,Port}).

%%%===================================================================
%%% gen_event callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% Whenever a new event handler is added to an event manager,
%% this function is called to initialize the event handler.
-spec init({atom(),inet:host(),inet:port_number()}) -> {ok, #state{}}.
init({Tag,Host,Port}) when is_atom(Tag) ->
    {ok,S} = try_connect(Host,Port,-1),
    TagBD = erlang:atom_to_list(Tag),
    {ok,#state{tag=Tag,tagbd=TagBD,host=Host,port=Port,sock=S}};
init(Tag) when is_atom(Tag) ->
  TagBD = erlang:atom_to_list(Tag),
  init({TagBD,localhost,24224}).

%% @private
-spec handle_event({ atom() | string() | binary(), tuple()}, #state{}) ->
                          {ok, #state{}} | remove_handler.
%%%===================================================================
%%% Lager-log format
%%%===================================================================
handle_event({log, _N, {Date, Time}, Data}, State) ->
    Bin = make_lager_package(Date, Time, Data, State),
    try_send(State, Bin, 3);

handle_event({<<"log">>, #lager_msg{datetime={Date, Time}, message=Message}}, State) ->
    Bin = make_lager_package(Date, Time, Message, State),
    try_send(State, Bin, 3);

%%%===================================================================
%%% Logger-log format
%%%===================================================================
handle_event({Label,Data}, State) when is_atom(Label) ->
    handle_event({erlang:atom_to_binary(Label, latin1),Data}, State);

handle_event({Label,Data}, State) when is_binary(Label) ->
    handle_event({erlang:binary_to_list(Label),Data}, State);

handle_event({Label,Data}, State) when is_binary(Label), is_tuple(Data) ->
    %% Data should be map
    Binary = case make_default_package(State, Label, Data) of
                 {error, _} ->  %% Data was not a map
                     make_error_package(State, <<"pack_error">>, Data);
                 Bin ->
                     Bin
             end,
    try_send(State, Binary, 3);

handle_event({Label,Data}, State) when is_binary(Label), is_list(Data) -> 
    %% map in proplist style
    Binary = case make_default_package_jsx(State, Label, Data) of
                 {error, _} ->  %% Data was not a map
                     make_error_package(State, <<"pack_error">>, Data);
                 Bin ->
                     Bin
             end,
    try_send(State, Binary, 3);

%%%===================================================================
%%% Direct data format (or other)
%%%===================================================================
handle_event(Other, State) ->
    Label = "direct",
    Data = {[{<<"log">>, erlang:list_to_binary(io_lib:format("~w", [Other]))}]},
    Bin = make_default_package(State, Label, Data),
    try_send(State, Bin, 3).

%%%===================================================================
%%% Other callbacks
%%%===================================================================
%% @private
handle_call(_Request, State) ->
    Reply = ok,
    {ok, Reply, State}.

%% @private
handle_info(_Info, State) ->
    {ok, State}.

%% @private
-spec terminate(atom(), #state{}) -> term().
terminate(_Reason, State) ->
    gen_tcp:close(State#state.sock).

%% @private
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec try_connect(inet:host(),inet:port_number(), integer()) -> {ok, inet:socket()}.
try_connect(_, _, 0) -> throw({error, retry_over});
try_connect(Host, Port, N) ->
    case gen_tcp:connect(Host, Port, [binary,{packet,0}]) of
        {ok, Sock} ->
            {ok, Sock};
        {error, _} ->
            timer:sleep(1000),
            try_connect(Host, Port, N-1)
    end.

-spec try_send(#state{}, binary(), non_neg_integer()) -> {ok, #state{}}.
try_send(_State, _, 0) -> throw({error, retry_over});
try_send(State, Bin, N) when is_binary(Bin) ->
    %% Here^^ uses matching with binary because successful msgpack:pack()
    %% always returns binary, not iolist().
    case gen_tcp:send(State#state.sock, Bin) of
        ok ->
            {ok, State};
        {error, closed} ->
            Host = State#state.host,
            Port = State#state.port,
            {ok,S} = try_connect(Host, Port, -1),
            try_send(State#state{sock=S}, Bin, N-1);
        Other ->
            throw({Other, Bin})
    end;
try_send(_State, {error, Reason}, _N) ->
    error(Reason).

%%%===================================================================
%%% MsgPack format package
%%%===================================================================
-spec make_default_package(#state{}, string(), msgpack:msgpack_map()) ->
                                binary() | {error, term()}.
make_default_package(State, Label, Data) ->
    make_default_package(State, Label, Data, []).

-spec make_default_package(#state{}, string(),
                           msgpack:object(),
                           msgpack:options()) ->
                                  binary() | {error, term()}.
make_default_package(State, Label, Data, PackOpt) ->
    {Msec,Sec,_} = os:timestamp(),
    Tag =  string:join([State#state.tagbd, Label], "."),
    Package = [Tag,
               Msec*1000000+Sec,
               Data,
               #{}],
    msgpack:pack(Package, PackOpt).

-spec make_error_package(#state{}, string(), term()) ->
                                  binary() | {error, term()}.
make_error_package(State, Label, Term) ->
    Data = erlang:list_to_binary(io_lib:format("~w", [Term])),
    make_default_package(State, Label, Data).

-spec make_default_package_jsx(#state{}, binary(), msgpack:object()) ->
                                  binary() | {error, term()}.
make_default_package_jsx(State, Label, Data) ->
    make_default_package(State, Label, Data, [{map_format, jsx}]).

%%%===================================================================
%%% Lager format package
%%%===================================================================
-spec make_lager_package(string(), string(),
                         msgpack:object(), #state{}) ->
                                binary() | {error, term()}.
make_lager_package(Date, Time, Data0, #state{tagbd=TagBD}) ->
    Label = <<"lager_log">>,
    Data = {[{<<"lager_date">>, erlang:list_to_binary(Date)},
             {<<"lager_time">>, erlang:list_to_binary(Time)},
             {<<"txt">>, erlang:list_to_binary(Data0)}]},
    {Msec,Sec,_} = os:timestamp(),
    Tag =  string:join([TagBD, Label], "."),
    Package = [Tag, Msec*1000000+Sec, Data, #{}],
    msgpack:pack(Package, []).
