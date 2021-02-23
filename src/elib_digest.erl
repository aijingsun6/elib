-module(elib_digest).

%% API
-export([
  md5_hex/1,
  file_md5_hex/1,
  sign/3
]).

md5_hex(Data) ->
  S = erlang:md5(Data),
  hex(S).

hex(Bin) ->
  iolist_to_binary([io_lib:format("~2.16.0b", [X]) || X <- binary_to_list(Bin)]).

file_md5_hex(File) ->
  case file:open(File, [binary, raw, read]) of
    {ok, IO} -> file_md5_hex2(IO, erlang:md5_init());
    Error -> Error
  end.

file_md5_hex2(IO, C) ->
  case file:read(IO, 1024) of
    {ok, Bin} ->
      file_md5_hex2(IO, erlang:md5_update(C, Bin));
    eof ->
      file:close(IO),
      {ok, hex(erlang:md5_final(C))}
  end.

sign(Type, PrivateKey, Body) when is_atom(Type), is_binary(PrivateKey), is_binary(Body) ->
  [PemEntry] = public_key:pem_decode(PrivateKey),
  PrivateKey2 = public_key:pem_entry_decode(PemEntry),
  Sign = public_key:sign(Body, sha256, PrivateKey2),
  base64:encode_to_string(Sign).