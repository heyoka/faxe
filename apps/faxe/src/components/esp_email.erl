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

-record(state, {
   node_id,
   from :: binary(),
   to :: list(),
   subject :: binary(),
   body,
   body_field,
   template,
   smtp_relay,
   smtp_user,
   smtp_pass
}).

options() -> [
   {from_address, binary, {email, from_address}},
   {smtp_relay, binary, {email, smtp_relay}},
   {smtp_user, any, {email, smtp_user}},
   {smtp_pass, any, {email, smtp_pass}},
   {template, binary, {email, template}},
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



init(NodeId, _Ins, #{
   from_address := From, smtp_relay := SmtpRelay, template := Template0,
   smtp_user := User, smtp_pass := Pass,
   to := To0, subject := Subj, body := Body, body_field := BodyField}) ->

   TemplateFile = binary_to_list(Template0),
   {ok, ContBin} = file:read_file(TemplateFile),

   State = #state{
      to = To0,
      subject = Subj,
      body = Body,
      body_field = BodyField,
      node_id = NodeId,
      from = From,
      smtp_relay = SmtpRelay,
      smtp_user = User,
      smtp_pass = Pass,
      template = ContBin
      },

   {ok, all, State}.

process(_In, P = #data_point{}, State = #state{from = From, to = To, subject = Subj}) ->
   Body = body(P, State),
   Res =
   gen_smtp_client:send(
      {From, To, mime(From, To, Subj, Body)},
      email_options(State)
   ),
   lager:notice("sent email to ~p, result: ~p",[To, Res]),
   {ok, State}.

email_options(#state{smtp_relay = Relay, smtp_user = User, smtp_pass = Pass}) ->
   Opts = [{relay, Relay}],
   case User of
      undefined -> Opts;
      _ -> Opts ++ [{username, User}, {password, Pass}]
   end.

body(P, S = #state{template = Template}) ->
   Content = content(P, S),
   binary:replace(Template, [<<"##PREHEADER##">>, <<"##CONTENT##">>], Content, [global]).

content(P, #state{body = Body, body_field = undefined}) ->
   string_template:eval(Body, P);
content(P, #state{body_field = BodyField}) ->
   flowdata:field(P, BodyField, <<"">>).

mime(From, To0, Subject, Body) ->
   To = iolist_to_binary(lists:join(<<",">>, To0)),
   mimemail:encode({<<"text">>, <<"html">>,
      [{<<"Subject">>, Subject},
         {<<"From">>, From},
         {<<"To">>, To}],
      #{content_type_params => [{<<"text">>, <<"html">>}]},
      Body
   }).

