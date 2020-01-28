%% Date: 27.01.20 - 18:38
%% Ⓒ 2020 heyoka
%% @doc
%% sends an email to 1 or more recipients
%% @end
-module(esp_email).
-author("Alexander Minichmair").

%% API
-behavior(df_component).

-include("faxe.hrl").
%% API
-export([init/3, process/3, options/0]).

-define(FROM_EMAIL, "noreply@tgw-group.com").

-record(state, {
   node_id,
   to,
   subject,
   body,
   body_field
}).

options() -> [
   {to, string_list},
   {subject, string},
   {body, string_template},
   {body_field, string}
].

init(NodeId, _Ins, #{to := To, subject := Subj, body := Body, body_field := BodyField}) ->
   State = #state{to = To, subject = Subj, body = Body, body_field = BodyField, node_id = NodeId},
   {ok, all, State}.

process(_In, P = #data_point{}, State = #state{to = To, subject = Subj, body = Body, body_field = _BodyField}) ->
   faxe_email:send(?FROM_EMAIL, To, Subj, Body),
   {ok, State}.
