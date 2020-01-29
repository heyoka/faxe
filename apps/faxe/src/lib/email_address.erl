-module(email_address).

-export([is_valid/1]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%-define(debug, ok).

-ifdef(debug).
-define(DEBUG(Format, Args),
   io:format("~s.~w: DEBUG: " ++ Format ++ "~n", [ ?MODULE, ?LINE | Args])).
-else.
-define(DEBUG(Format, Args), true).
-endif.

% Email addresses are ascii unless there is the RFC 6530/RFC 6531 extension in effect.

% The local part has few rules:
%   Uppercase and lowercase English letters (a–z, A–Z) (ASCII: 65–90, 97–122)
%   Digits 0 to 9 (ASCII: 48–57)
%   Characters !#$%&'*+-/=?^_`{|}~ (ASCII: 33, 35–39, 42, 43, 45, 47, 61, 63, 94–96, 123–126)
%   Character . (dot, period, full stop) (ASCII: 46) provided that it is not the first or last character, and provided also that it does not appear two or more times consecutively (e.g. John..Doe@example.com is not allowed.).
%   Special characters are allowed with restrictions. They are:
%   Space and "(),:;<>@[\] (ASCII: 32, 34, 40, 41, 44, 58, 59, 60, 62, 64, 91–93)
%   The restrictions for special characters are that they must only be used when contained between quotation marks, and that 2 of them (the backslash \ and quotation mark " (ASCII: 32, 92, 34)) must also be preceded by a backslash \ (e.g. "\\\"").
%   Comments are allowed with parentheses at either end of the local part; e.g. "john.smith(comment)@example.com" and "(comment)john.smith@example.com" are both equivalent to "john.smith@example.com".
%   International characters above U+007F are permitted by RFC 6531, though mail systems may restrict which characters to use when assigning local parts.
% The resulting regex below is only a weak approximation of these rules so far. Consider it work in progress.
% TODO: need to support the RFC 6530/RFC 6531 extensions in the future. So far we don't support unicode.
% TODO: process comment correctly (filter them out)

is_valid(Addr) when is_binary(Addr) ->
   is_valid(binary_to_list(Addr));
is_valid(Addr) when is_list(Addr) ->
   {ok, LocalMP} = re:compile("^((?:(?:[^\"@\\.]+\\.?)|(?:\\.\"[^\"]+\"\\.))*(?:(?:\\.?\"[^\"]+\")|(?:[a-zA-Z0-9\\-_]+)))@[a-z0-9\\.\\-\\[\\]]+$", [caseless, anchored]),
   Match = re:run(Addr, LocalMP),
   ?DEBUG("Match: ~p", [Match]),
   is_valid(Match, Addr).

is_valid(nomatch, _) ->
   false;
is_valid({match,Ranges}, Addr) when length(Ranges) =:= 2 ->
   {Start, End} = lists:nth(2, Ranges),
   ?DEBUG("range: ~p to ~p", [Start, End]),
   is_valid(Start, End, Addr).

is_valid(0, End, Addr) when End > 0 ->
   ?DEBUG("~p[0..~p]", [Addr, End]),
   domain_is_valid(string:sub_string(Addr, End+2));
is_valid(_, _, _) ->
   false.

% TODO: need to fetch the TLDs dynamically from: http://data.iana.org/TLD/tlds-alpha-by-domain.txt
domain_is_valid(FQDN) ->
   ?DEBUG("matching ~p", [FQDN]),
   DomRegex = "^((?:(?:[A-Z0-9][A-Z0-9\-]+\\.)+(?:AC|ACTOR|AD|AE|AERO|AF|AG|AI|AL|AM|AN|AO|AQ|AR|ARPA|AS|ASIA|AT|AU|AW|AX|AZ|BA|BB|BD|BE|BF|BG|BH|BI|BIZ|BJ|BM|BN|BO|BR|BS|BT|BV|BW|BY|BZ|CA|CAT|CC|CD|CF|CG|CH|CI|CK|CL|CM|CN|CO|COM|COOP|CR|CU|CV|CW|CX|CY|CZ|DE|DJ|DK|DM|DO|DZ|EC|EDU|EE|EG|ER|ES|ET|EU|FI|FJ|FK|FM|FO|FR|GA|GB|GD|GE|GF|GG|GH|GI|GL|GM|GN|GOV|GP|GQ|GR|GS|GT|GU|GW|GY|HK|HM|HN|HR|HT|HU|ID|IE|IL|IM|IN|INFO|INT|IO|IQ|IR|IS|IT|JE|JM|JO|JOBS|JP|KE|KG|KH|KI|KM|KN|KP|KR|KW|KY|KZ|LA|LB|LC|LI|LK|LR|LS|LT|LU|LV|LY|MA|MC|MD|ME|MEDIA|MG|MH|MIL|MK|ML|MM|MN|MO|MOBI|MP|MQ|MR|MS|MT|MU|MUSEUM|MV|MW|MX|MY|MZ|NA|NAME|NC|NE|NET|NF|NG|NI|NL|NO|NP|NR|NU|NZ|OM|ORG|PA|PATEL|PE|PF|PG|PH|PK|PL|PM|PN|POST|PR|PRO|PS|PT|PW|PY|QA|RE|RO|RS|RU|RW|SA|SB|SC|SD|SE|SG|SH|SI|SJ|SK|SL|SM|SN|SO|SR|ST|SU|SV|SX|SY|SZ|TC|TD|TEL|TF|TG|TH|TJ|TK|TL|TM|TN|TO|TP|TR|TRAVEL|TT|TV|TW|TZ|UA|UG|UK|US|UY|UZ|VA|VC|VE|VG|VI|VN|VU|WF|WS|XN--0ZWM56D|XN--11B5BS3A9AJ6G|XN--3E0B707E|XN--45BRJ9C|XN--80AKHBYKNJ4F|XN--80AO21A|XN--90A3AC|XN--9T4B11YI5A|XN--CLCHC0EA0B2G2A9GCD|XN--DEBA0AD|XN--FIQS8S|XN--FIQZ9S|XN--FPCRJ9C3D|XN--FZC2C9E2C|XN--G6W251D|XN--GECRJ9C|XN--H2BRJ9C|XN--HGBK6AJ7F53BBA|XN--HLCJ6AYA9ESC7A|XN--J6W193G|XN--JXALPDLP|XN--KGBECHTV|XN--KPRW13D|XN--KPRY57D|XN--LGBBAT1AD8J|XN--MGB9AWBF|XN--MGBAAM7A8H|XN--MGBAYH7GPA|XN--MGBBH1A71E|XN--MGBC0A9AZCG|XN--MGBERP4A5D4AR|XN--MGBX4CD0AB|XN--O3CW4H|XN--OGBPF8FL|XN--P1AI|XN--PGBS0DH|XN--S9BRJ9C|XN--WGBH1C|XN--WGBL6A|XN--XKC2AL3HYE2A|XN--XKC2DL3A5EE0H|XN--YFRO4I67O|XN--YGBI2AMMX|XN--ZCKZAH|XXX|YE|YT|ZA|ZM|ZW))|(?:[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3})|(?:\\[[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\]))$",
   case re:run(FQDN,DomRegex,[caseless, anchored]) of
      {match, _Captured} ->
         ?DEBUG("Captured: ~p", [_Captured]),
         true;
      matched ->
         true;
      nomatch ->
         false
   end.
