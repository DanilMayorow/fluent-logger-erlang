%%%-------------------------------------------------------------------
%%% @author dannmaj <dan.major.work@gmail.com>
%%% @copyright (C) 2022, Eltex
%%% @doc
%%%
%%% @end
%%% Created : 27. Sep 2022 17:15 by dannmaj 
%%%-------------------------------------------------------------------

-define(SERVER, ?MODULE).

-record(state, {
  tag  :: atom(),
  tagbd :: string(), % string of "tag."
  host :: inet:host(),
  port :: inet:port_number(),
  sock :: inet:socket()
}).

% for lager 2.0 format
-record(lager_msg,{
  destinations :: list(),
  metadata :: [tuple()],
  severity :: atom(),
  datetime :: {string(), string()},
  timestamp :: erlang:timestamp(),
  message :: list()
}).
