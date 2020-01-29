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
-export([init/3, process/3, options/0, check_options/0]).


-define(FROM_EMAIL, <<"noreply@tgw-group.com">>).
-define(SMTP_RELAY, <<"smtp.tgw.local">>).

-record(state, {
   node_id,
   from :: binary(),
   to :: list(),
   subject :: binary(),
   body,
   body_field,
   smtp_relay,
   smtp_user,
   smtp_pass
}).

options() -> [
   {to, string_list},
   {subject, string},
   {body, string_template, undefined},
   {body_field, string, undefined}
].

check_options() ->
   [
      {one_of_params, [body, body_field]},
      {func, to, fun check_email/1, <<"invalid email address(es) given">>}
   ].

check_email(Address) when is_list(Address) ->
   lists:all(fun(A) -> email_address:is_valid(A) end, Address);
check_email(Address) when is_binary(Address) ->
   email_address:is_valid(Address).



init(NodeId, _Ins, #{to := To0, subject := Subj, body := Body, body_field := BodyField}) ->
   To = [<<"<", Address/binary, ">">> || Address <- To0],

   EmailData = application:get_env(faxe, email, []),
   From = proplists:get_value(from_address, EmailData, ?FROM_EMAIL),
   SmtpRelay = proplists:get_value(smtp_relay, EmailData, ?SMTP_RELAY),

   State = #state{
      to = To,
      subject = Subj,
      body = Body,
      body_field = BodyField,
      node_id = NodeId,
      from = From,
      smtp_relay = SmtpRelay,
      smtp_user = undefined,
      smtp_pass = undefined
      },

   {ok, all, State}.

process(_In, P = #data_point{}, State = #state{from = From, to = To, subject = Subj}) ->
   Body = body(P, State),
   gen_smtp_client:send({From, To, build_body(
      binary_to_list(Subj), binary_to_list(From), To, binary_to_list(Body))},
      email_options(State)
   ),
   {ok, State}.

email_options(#state{smtp_relay = Relay, smtp_user = User, smtp_pass = Pass}) ->
   Opts = [{relay, Relay}],
   case User of
      undefined -> Opts;
      _ -> Opts ++ [{username, User}, {password, Pass}]
   end.

body(P, #state{body = Body, body_field = undefined}) ->
   string_template:eval(Body, P)
;
body(P, #state{body_field = BodyField}) ->
   flowdata:field(P, BodyField, <<"">>).

build_body(Subject, From, To, Message) ->
   Format = "Subject: ~s\r\nFrom: <~s>\r\nTo: <~s>\r\n\r\n~s",
   B = io_lib:format(Format, [Subject, From, To, Message]),
   lager:info("email body: ~p", [B]),
   B.

