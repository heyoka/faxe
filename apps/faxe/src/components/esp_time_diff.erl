%% Ⓒ 2019 heyoka
%%
%% @doc
%% The time_diff node adds a field to the current data-item
%% containing the difference between the timestamps of the consecutive items
%%
%%
%% With the 'as' option, the name of the output field can be changed.
%%
%% 'as' defaults to "timediff"
%%
%% unit for output value is milliseconds
%% @end
-module(esp_time_diff).
-author("Alexander Minichmair").

%% API
-behavior(df_component).

-include("faxe.hrl").
%% API
-export([init/3, process/3, options/0]).

-record(state, {
   node_id,
   as,
   last_timestamp
}).

options() -> [{as, binary, <<"timediff">>}].

init(NodeId, _Ins, #{as := As}) ->
   {ok, all, #state{node_id = NodeId, as = As}}.

process(_In, #data_batch{points = Points} = B, State = #state{last_timestamp = LT, as = As}) ->
   {NewPoints, LastTime} = process_points(Points, LT, As),
   {emit, B#data_batch{points =  NewPoints}, State#state{last_timestamp = LastTime}};
process(_In, #data_point{ts = Ts} = P, State = #state{last_timestamp = undefined, as = As}) ->
   {emit, flowdata:set_field(P, As, -1), State#state{last_timestamp = Ts}};
process(_In, #data_point{ts = Ts} = Item, State = #state{as = As, last_timestamp = Last}) ->
   NewItem = flowdata:set_field(Item, As, Ts - Last),
   {emit, NewItem, State#state{last_timestamp = Ts}}.

process_points(Points, Last, As) ->
   lists:mapfoldr(
      fun(#data_point{ts = Ts} = P, LastTime) ->
         Diff =
         case LastTime of
            undefined -> -1;
            _ -> Ts - LastTime
         end,
         {flowdata:set_field(P, As, Diff), Ts}
            end,
      Last,
      Points

   ).

%%%%%%%%%%%%
-ifdef(TEST).

process_point_first_test() ->
   faxe_ets:start_link(),
   Points = [
      #data_point{ts=1569483988616,fields=#{<<"val">> => 8.00511212301019}},
      #data_point{ts=1569483985616,fields=#{<<"val">> => 1.378982487661966}},
      #data_point{ts=1569483982616,fields=#{<<"val">> => 5.636442755846095}},
      #data_point{ts=1569483979616,fields=#{<<"val">> => 4.088178983215122}},
      #data_point{ts=1569483976616,fields=#{<<"val">> => 1.4082696296046215}}
   ],
   OutPoints = [
      #data_point{ts=1569483988616,fields=#{<<"val">> => 8.00511212301019, <<"timediff">> => 3000}},
      #data_point{ts=1569483985616,fields=#{<<"val">> => 1.378982487661966, <<"timediff">> => 3000}},
      #data_point{ts=1569483982616,fields=#{<<"val">> => 5.636442755846095, <<"timediff">> => 3000}},
      #data_point{ts=1569483979616,fields=#{<<"val">> => 4.088178983215122, <<"timediff">> => 3000}},
      #data_point{ts=1569483976616,fields=#{<<"val">> => 1.4082696296046215, <<"timediff">> => -1}}
   ],
   ?assertEqual({OutPoints, 1569483988616}, process_points(Points, undefined, <<"timediff">>)).

process_points_test() ->
   faxe_ets:start_link(),
   Points = [
      #data_point{ts=1569483988616,fields=#{<<"val">> => 8.00511212301019}},
      #data_point{ts=1569483985616,fields=#{<<"val">> => 1.378982487661966}},
      #data_point{ts=1569483982616,fields=#{<<"val">> => 5.636442755846095}},
      #data_point{ts=1569483979616,fields=#{<<"val">> => 4.088178983215122}},
      #data_point{ts=1569483976616,fields=#{<<"val">> => 1.4082696296046215}}
   ],
   OutPoints = [
      #data_point{ts=1569483988616,fields=#{<<"val">> => 8.00511212301019, <<"timediff">> => 3000}},
      #data_point{ts=1569483985616,fields=#{<<"val">> => 1.378982487661966, <<"timediff">> => 3000}},
      #data_point{ts=1569483982616,fields=#{<<"val">> => 5.636442755846095, <<"timediff">> => 3000}},
      #data_point{ts=1569483979616,fields=#{<<"val">> => 4.088178983215122, <<"timediff">> => 3000}},
      #data_point{ts=1569483976616,fields=#{<<"val">> => 1.4082696296046215, <<"timediff">> => 3000}}
   ],
   ?assertEqual({OutPoints, 1569483988616}, process_points(Points, 1569483973616, <<"timediff">>)).

-endif.