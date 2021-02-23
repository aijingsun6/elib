-module(elib_digest_test).

-include_lib("eunit/include/eunit.hrl").

md5_test()->
  MD5 = "123456",
  ?assertEqual(<<"e10adc3949ba59abbe56e057f20f883e">>,elib_digest:md5_hex(MD5)).
