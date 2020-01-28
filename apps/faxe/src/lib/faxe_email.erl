%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 28. Jän 2020 20:40
%%%-------------------------------------------------------------------
-module(faxe_email).
-author("heyoka").

%% API
-export([send/0, send/4]).

send() ->
   send("erdverbindung@gmx.at", "erdverbindung@gmx.at", "mail from FAXE", "hello from FAXE").
send(From, To, Subject, Body) ->
   gen_smtp_client:send({From, [To], build_body(Subject, From, To, Body)},
   [{relay, "smtp.gmx.com"}, {username, "erdverbindung@gmx.at"}, {password, "feinstofflich"}]).

build_body(Subject, From, To, Message) ->
   Format = "Subject: ~s\r\nFrom: <~s>\r\nTo: <~s>\r\n\r\n~s",
   B = io_lib:format(Format, [Subject, From, To, Message]),
   lager:info("email body: ~p", [B]),
   B.
