%% @doc Upload handler.
-module(upload_handler).

-export([init/2, from_data/2, allowed_methods/2, content_types_accepted/2, is_authorized/2, valid_entity_length/2]).


-include("faxe.hrl").

-define(UPLOAD_TIMEOUT, 50000). %% 50sec timeout for upload to backend
-define(MAX_UPLOAD_SIZE_BYTES, 5000000). %% ca 5 Mib

init(Req, [{op, Mode}]) ->
   lager:notice("upload handler: ~p", [Mode]),
   {cowboy_rest, Req, Mode}.

valid_entity_length(Req=#{body_length := Length}, State) ->
    Value = faxe_config:get(max_upload_size, ?MAX_UPLOAD_SIZE_BYTES) >= Length,
    {Value, Req, State}.

allowed_methods(Req, State) ->
   {[<<"POST">>], Req, State}.

content_types_accepted(Req = #{method := <<"POST">>}, State) ->
   Value = [{{ <<"multipart">>, <<"form-data">>, []}, from_data}],
   {Value, Req, State}.

is_authorized(Req, State) ->
   rest_helper:is_authorized(Req, State).

from_data(Req, State) ->
   case multipart(Req) of
      {ok, Req1, Data} ->
         ResponseList = save_data(Data, State),
         Req2 = cowboy_req:set_resp_header(<<"content-type">>, <<"application/json">>, Req1),
         Req3 = cowboy_req:set_resp_body(jiffy:encode(#{files => ResponseList}), Req2),
         {true, Req3, State};
      {error, Reason, Req1} ->
         Req3 = cowboy_req:set_resp_body(Reason, Req1),
         {false, Req3, State}
   end.

save_data(Files, Mode) ->
   Path0 = get_filepath(Mode),
   Fun =
      fun({FileName, Bin}, Acc) ->
         Path = filename:join(Path0, FileName),
         case file:write_file(Path, Bin) of
            ok -> [#{uploaded => FileName, stored => Path} | Acc];
            {error, What} -> [#{error => FileName, message => What } | Acc]
         end
      end,
   lists:foldl(Fun, [], Files).

get_filepath(Mode) ->
   Ops = faxe_config:get(Mode),
   proplists:get_value(script_path, Ops).


multipart(Req) ->
   multipart(Req, []).

multipart(Req0, Acc) ->
   case cowboy_req:read_part(Req0, #{length => 2000000, period => 10000}) of
      {ok, Headers, Req1} ->
         case cow_multipart:form_data(Headers) of
%%            {data, FieldName} ->
%%               {ok, Body, Req2} = cowboy_req:read_part_body(Req1),
%%               [_FName, Pos] = extract_field_name(FieldName),
%%               io:format("multipart data: ~p: ~p~n", [{_FName, Body}, Pos]),
%%               multipart(Req2, Acc#{params => Params ++ [{Pos, decode_meta(Body)}] });
            {file, _FieldName, <<>>, _CType} ->
               io:format("multipart file empty: ~p~n", [{_FieldName}]),
               multipart(Req1, Acc);
            {file, FieldName, Filename, CType} ->
               io:format("multipart file: ~p~n", [{FieldName, Filename, CType}]),
%%               [_FName, Pos] = extract_field_name(FieldName),
               UploadFile = <<>>,
%%                  #file{mime = CType, filename = Filename,
%%                  created = erlang:system_time(millisecond)
%%                  , id = uuid:uuid_to_string(uuid:get_v4(strong))
%%               },
               case handle_file(Req1, UploadFile) of
                  {ok, Req2, NewFile} ->
                     multipart(Req2, Acc ++ [{Filename, NewFile}]); %#{files => Files ++  [{Pos, NewFile}]});
                  Other ->
                     Other
               end;
            _Other ->
               lager:warning("multipart reports: ~p",[_Other]),
               multipart(Req1, Acc)
         end;
      {done, Req} ->
         {ok, Req, Acc}
   end.


%%decode_meta(undefined) -> #{};
%%decode_meta(Body) when is_binary(Body) -> jiffy:decode(Body, [return_maps]).

handle_file(Req1, File) ->
   {Req2, NewFile} = stream_file(Req1, File),
   {ok, Req2, NewFile}.

stream_file(Req, File) ->
   case catch(do_stream_file(Req, File, <<>>)) of
      {ok, Req2, File1} ->
         {Req2, File1};
      Err ->
         io:format("Error when streaming file: ~p~n",[Err]),
         {Req, 0}
   end.

do_stream_file(Req0, File, BinAcc) ->
   case cowboy_req:read_part_body(Req0, #{length => 1000000, period => 7000}) of
      {ok, LastBodyChunk, Req} ->
         Bin = <<BinAcc/binary, LastBodyChunk/binary>>,
         NewFile = Bin,
         {ok, Req, NewFile};
      {more, BodyChunk, Req} ->
%%         logger:info("more data ..."),
         do_stream_file(Req, File, <<BinAcc/binary, BodyChunk/binary>>)
   end.

