%% Date: 02.05.17 - 21:15
%% Ⓒ 2017 heyoka
-module(faxe_web_handler).
-author("Alexander Minichmair").

%% Standard callbacks.
-export([init/3]).
-export([allowed_methods/2]).
-export([content_types_provided/2]).
-export([content_types_accepted/2]).
%%-export([resource_exists/2]).

%% Custom callbacks.
-export([create_paste/2]).
-export([paste_html/2, task_list/2]).
-export([paste_text/2]).

init(_Transport, _Req, []) ->
   random:seed(),
   {upgrade, protocol, cowboy_rest}.

allowed_methods(Req, State) ->
   {[<<"GET">>, <<"POST">>], Req, State}.

content_types_provided(Req, State) ->
   {[
      {{<<"text">>, <<"html">>, []}, task_list}
   ], Req, State}.

content_types_accepted(Req, State) ->
   {[{{<<"application">>, <<"x-www-form-urlencoded">>, []}, create_paste}],
      Req, State}.

%%resource_exists(Req, _State) ->
%%   case cowboy_req:binding(paste_id, Req) of
%%      {undefined, Req2} ->
%%         {true, Req2, index};
%%      {PasteID, Req2} ->
%%         case valid_path(PasteID) and file_exists(PasteID) of
%%            true -> {true, Req2, PasteID};
%%            false -> {false, Req2, PasteID}
%%         end
%%   end.

task_list(Req, State) ->
   TaskList = faxe:list_tasks(),
   {ok, Body} = templates_side:render([]), %<<"jo">>,
   lager:notice("Body rendered: ~p~n",[Body]),
%%   {ok, Body} = render([{list, TaskList}]),
   {ok, Req2} = cowboy_req:reply(200, [{<<"content-type">>, <<"text/html">>}], iolist_to_binary(Body), Req),
   {ok, Req2, State}.

create_paste(Req, State) ->
   PasteID = new_paste_id(),
   {ok, [{<<"paste">>, Paste}], Req3} = cowboy_req:body_qs(Req),
   ok = file:write_file(full_path(PasteID), Paste),
   case cowboy_req:method(Req3) of
      {<<"POST">>, Req4} ->
         {{true, <<$/, PasteID/binary>>}, Req4, State};
      {_, Req4} ->
         {true, Req4, State}
   end.

paste_html(Req, _) ->
   {read_file("empty.html"), Req, index};
paste_html(Req, Paste) ->
   {Style, Req2} = cowboy_req:qs_val(<<"lang">>, Req, plain),
   {format_html(Paste, Style), Req2, Paste}.

paste_text(Req, index) ->
   {read_file("index.txt"), Req, index};
paste_text(Req, Paste) ->
   {Style, Req2} = cowboy_req:qs_val(<<"lang">>, Req, plain),
   {format_text(Paste, Style), Req2, Paste}.

% Private

read_file(Name) ->
   {ok, Binary} = file:read_file(full_path(Name)),
   Binary.

full_path(Name) ->
   filename:join([code:priv_dir(faxe), Name]).

file_exists(Name) ->
   case file:read_file_info(full_path(Name)) of
      {ok, _Info} -> true;
      {error, _Reason} -> false
   end.

valid_path(<<>>) -> true;
valid_path(<<$., _T/binary>>) -> false;
valid_path(<<$/, _T/binary>>) -> false;
valid_path(<<_Char, T/binary>>) -> valid_path(T).

new_paste_id() ->
   Initial = random:uniform(62) - 1,
   new_paste_id(<<Initial>>, 7).
new_paste_id(Bin, 0) ->
   Chars = <<"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890">>,
   << <<(binary_part(Chars, B, 1))/binary>> || <<B>> <= Bin >>;
new_paste_id(Bin, Rem) ->
   Next = random:uniform(62) - 1,
   new_paste_id(<<Bin/binary, Next>>, Rem - 1).

format_html(Paste, plain) ->
   Text = escape_html_chars(read_file(Paste)),
   <<"<!DOCTYPE html><html>",
      "<head><title>paste</title></head>",
      "<body><pre><code>", Text/binary, "</code></pre></body></html>\n">>;
format_html(Paste, Lang) ->
   highlight(full_path(Paste), Lang, "html").

format_text(Paste, plain) ->
   read_file(Paste);
format_text(Paste, Lang) ->
   highlight(full_path(Paste), Lang, "ansi").

highlight(Path, Lang, Type) ->
   Path1 = binary_to_list(Path),
   Lang1 = binary_to_list(Lang),
   os:cmd(["highlight --syntax=", Lang1,
      " --doc-title=paste ",
      " --out-format=", Type,
      " --include-style ", Path1]).

% Escape some HTML characters that might make a fuss
escape_html_chars(Bin) ->
   << <<(escape_html_char(B))/binary>> || <<B>> <= Bin >>.

escape_html_char($<) -> <<"&lt;">>;
escape_html_char($>) -> <<"&gt;">>;
escape_html_char($&) -> <<"&amp;">>;
escape_html_char(C) -> <<C>>.
