-module(elib_convert).

%% API
-export([
  to_string/1,
  to_binary/1,
  to_integer/1,
  to_float/1,
  to_boolean/1
]).

-export([
  term_to_string/1,
  string_to_term/1
]).

to_string(A) when erlang:is_atom(A) -> erlang:atom_to_list(A);
to_string(I) when is_integer(I) -> erlang:integer_to_list(I);
to_string(F) when is_float(F) -> lists:flatten(io_lib:format("~p", [F]));
to_string(L) when is_list(L) -> L;
to_string(B) when is_binary(B) -> erlang:binary_to_list(B);
to_string(T) -> lists:flatten(io_lib:format("~p", [T])).


to_binary(A) when is_atom(A) -> erlang:atom_to_binary(A, utf8);
to_binary(I) when is_integer(I) -> erlang:integer_to_binary(I);
to_binary(F) when is_float(F) -> to_binary(to_string(F));
to_binary(B) when is_binary(B) -> B;
to_binary(L) when is_list(L) -> unicode:characters_to_binary(L);
to_binary(T) -> erlang:term_to_binary(T).

to_integer(I) when is_integer(I) -> I;
to_integer(F) when is_float(F) -> erlang:round(F);
to_integer(L) when is_list(L) -> erlang:list_to_integer(L);
to_integer(B) when is_binary(B) -> erlang:binary_to_integer(B).

to_float(F) when is_float(F) -> F;
to_float(I) when is_integer(I) -> I + 0.0;
to_float(L) when is_list(L) -> erlang:list_to_float(L);
to_float(B) when is_binary(B) -> erlang:binary_to_float(B).

to_boolean(B) when is_boolean(B) -> B;
to_boolean(I) when is_integer(I) -> I =/= 0;
to_boolean("true") -> true;
to_boolean(<<"true">>) -> true;
to_boolean("false") -> false;
to_boolean(<<"false">>) -> false.

term_to_string(Term) ->
  lists:flatten(io_lib:format("~p", [Term])).

%% {ok, [Term...]}
string_to_term(Str) ->
  case erl_scan:string(Str) of
    {ok, L, _} -> parse_terms(L, []);
    Err -> Err
  end.

parse_terms(Tokens, Acc) ->
  case lists:splitwith(fun(T) -> element(1, T) =/= dot end, Tokens) of
    {[], []} ->
      {ok, lists:reverse(Acc)};
    {Tokens2, [{dot, _} = Dot | Rest]} ->
      case erl_parse:parse_term(Tokens2 ++ [Dot]) of
        {ok, Term} ->
          parse_terms(Rest, [Term | Acc]);
        {error, _R} = Error ->
          Error
      end;
    {Tokens2, []} ->
      case erl_parse:parse_term(Tokens2 ++ [{dot, 1}]) of
        {ok, Term} ->
          {ok, lists:reverse([Term | Acc])};
        {error, _R} = Error ->
          Error
      end
  end.

