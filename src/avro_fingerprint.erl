%% coding: latin-1
%%%-----------------------------------------------------------------------------
%%%
%%% Copyright (c) 2018 Klarna AB
%%%
%%% This file is provided to you under the Apache License,
%%% Version 2.0 (the "License"); you may not use this file
%%% except in compliance with the License.  You may obtain
%%% a copy of the License at
%%%
%%%   http://www.apache.org/licenses/LICENSE-2.0
%%%
%%% Unless required by applicable law or agreed to in writing,
%%% software distributed under the License is distributed on an
%%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%%% KIND, either express or implied.  See the License for the
%%% specific language governing permissions and limitations
%%% under the License.
%%%
%%% @author Jake Morrison <jake@cogini.com>
%%%
%%% @doc
%%% Implement the Avro CRC 64 fingerprint algorithm.
%%%
%%% The algorithm is defined with this Java code:
%%%
%%%  ```
%%%  static long fingerprint64(byte[] buf) {
%%%    if (FP_TABLE == null) initFPTable();
%%%    long fp = EMPTY;
%%%    for (int i = 0; i < buf.length; i++)
%%%      fp = (fp >>> 8) ^ FP_TABLE[(int)(fp ^ buf[i]) & 0xff];
%%%    return fp;
%%%  }
%%%
%%%  static long EMPTY = 0xc15d213aa4d7a795L;
%%%  static long[] FP_TABLE = null;
%%%
%%%  static void initFPTable() {
%%%    FP_TABLE = new long[256];
%%%    for (int i = 0; i < 256; i++) {
%%%      long fp = i;
%%%      for (int j = 0; j < 8; j++)
%%%        fp = (fp >>> 1) ^ (EMPTY & -(fp & 1L));
%%%      FP_TABLE[i] = fp;
%%%      // Generate the lookup table used in this code
%%%      System.out.format("fp_table(%d) -> 16#%s;%n", i, Long.toHexString(fp));
%%%    }
%%%  }
%%%  '''
%%% @end
%%%
%%% @reference See <a href="https://avro.apache.org/docs/1.8.2/spec.html#
%%% schema_fingerprints">The Avro Spec</a> for more information.
%%% @reference See <a
%%% href="https://avro.apache.org/docs/1.8.2/api/java/org/apache/avro/
%%% SchemaNormalization.html">The source</a>
%%%-----------------------------------------------------------------------------
-module(avro_fingerprint).

%% API
-export([crc64/1]).

-type crc64() :: non_neg_integer().
-export_type([crc64/0]).

-spec crc64(binary()) -> non_neg_integer().
crc64(Data) ->
  crc64(Data, 16#c15d213aa4d7a795).

-spec crc64(binary(), non_neg_integer()) -> non_neg_integer().
crc64(<<Byte, Rest/binary>>, Fp) ->
  Fp1 = (Fp bsr 8) bxor fp_table((Fp bxor Byte) band 16#ff),
  crc64(Rest, Fp1);
crc64(<<>>, Fp) ->
  Fp.

-spec fp_table(non_neg_integer()) -> non_neg_integer().
fp_table(0) -> 16#0;
fp_table(1) -> 16#2cf1cba6b75351fa;
fp_table(2) -> 16#59e3974d6ea6a3f4;
fp_table(3) -> 16#75125cebd9f5f20e;
fp_table(4) -> 16#b3c72e9add4d47e8;
fp_table(5) -> 16#9f36e53c6a1e1612;
fp_table(6) -> 16#ea24b9d7b3ebe41c;
fp_table(7) -> 16#c6d5727104b8b5e6;
fp_table(8) -> 16#e5341f40f335c0fb;
fp_table(9) -> 16#c9c5d4e644669101;
fp_table(10) -> 16#bcd7880d9d93630f;
fp_table(11) -> 16#902643ab2ac032f5;
fp_table(12) -> 16#56f331da2e788713;
fp_table(13) -> 16#7a02fa7c992bd6e9;
fp_table(14) -> 16#0f10a69740de24e7;
fp_table(15) -> 16#23e16d31f78d751d;
fp_table(16) -> 16#48d27cf4afc4cedd;
fp_table(17) -> 16#6423b75218979f27;
fp_table(18) -> 16#1131ebb9c1626d29;
fp_table(19) -> 16#3dc0201f76313cd3;
fp_table(20) -> 16#fb15526e72898935;
fp_table(21) -> 16#d7e499c8c5dad8cf;
fp_table(22) -> 16#a2f6c5231c2f2ac1;
fp_table(23) -> 16#8e070e85ab7c7b3b;
fp_table(24) -> 16#ade663b45cf10e26;
fp_table(25) -> 16#8117a812eba25fdc;
fp_table(26) -> 16#f405f4f93257add2;
fp_table(27) -> 16#d8f43f5f8504fc28;
fp_table(28) -> 16#1e214d2e81bc49ce;
fp_table(29) -> 16#32d0868836ef1834;
fp_table(30) -> 16#47c2da63ef1aea3a;
fp_table(31) -> 16#6b3311c55849bbc0;
fp_table(32) -> 16#91a4f9e95f899dba;
fp_table(33) -> 16#bd55324fe8dacc40;
fp_table(34) -> 16#c8476ea4312f3e4e;
fp_table(35) -> 16#e4b6a502867c6fb4;
fp_table(36) -> 16#2263d77382c4da52;
fp_table(37) -> 16#0e921cd535978ba8;
fp_table(38) -> 16#7b80403eec6279a6;
fp_table(39) -> 16#57718b985b31285c;
fp_table(40) -> 16#7490e6a9acbc5d41;
fp_table(41) -> 16#58612d0f1bef0cbb;
fp_table(42) -> 16#2d7371e4c21afeb5;
fp_table(43) -> 16#0182ba427549af4f;
fp_table(44) -> 16#c757c83371f11aa9;
fp_table(45) -> 16#eba60395c6a24b53;
fp_table(46) -> 16#9eb45f7e1f57b95d;
fp_table(47) -> 16#b24594d8a804e8a7;
fp_table(48) -> 16#d976851df04d5367;
fp_table(49) -> 16#f5874ebb471e029d;
fp_table(50) -> 16#809512509eebf093;
fp_table(51) -> 16#ac64d9f629b8a169;
fp_table(52) -> 16#6ab1ab872d00148f;
fp_table(53) -> 16#464060219a534575;
fp_table(54) -> 16#33523cca43a6b77b;
fp_table(55) -> 16#1fa3f76cf4f5e681;
fp_table(56) -> 16#3c429a5d0378939c;
fp_table(57) -> 16#10b351fbb42bc266;
fp_table(58) -> 16#65a10d106dde3068;
fp_table(59) -> 16#4950c6b6da8d6192;
fp_table(60) -> 16#8f85b4c7de35d474;
fp_table(61) -> 16#a3747f616966858e;
fp_table(62) -> 16#d666238ab0937780;
fp_table(63) -> 16#fa97e82c07c0267a;
fp_table(64) -> 16#a1f3b1a7f6bc745f;
fp_table(65) -> 16#8d027a0141ef25a5;
fp_table(66) -> 16#f81026ea981ad7ab;
fp_table(67) -> 16#d4e1ed4c2f498651;
fp_table(68) -> 16#12349f3d2bf133b7;
fp_table(69) -> 16#3ec5549b9ca2624d;
fp_table(70) -> 16#4bd7087045579043;
fp_table(71) -> 16#6726c3d6f204c1b9;
fp_table(72) -> 16#44c7aee70589b4a4;
fp_table(73) -> 16#68366541b2dae55e;
fp_table(74) -> 16#1d2439aa6b2f1750;
fp_table(75) -> 16#31d5f20cdc7c46aa;
fp_table(76) -> 16#f700807dd8c4f34c;
fp_table(77) -> 16#dbf14bdb6f97a2b6;
fp_table(78) -> 16#aee31730b66250b8;
fp_table(79) -> 16#8212dc9601310142;
fp_table(80) -> 16#e921cd535978ba82;
fp_table(81) -> 16#c5d006f5ee2beb78;
fp_table(82) -> 16#b0c25a1e37de1976;
fp_table(83) -> 16#9c3391b8808d488c;
fp_table(84) -> 16#5ae6e3c98435fd6a;
fp_table(85) -> 16#7617286f3366ac90;
fp_table(86) -> 16#03057484ea935e9e;
fp_table(87) -> 16#2ff4bf225dc00f64;
fp_table(88) -> 16#0c15d213aa4d7a79;
fp_table(89) -> 16#20e419b51d1e2b83;
fp_table(90) -> 16#55f6455ec4ebd98d;
fp_table(91) -> 16#79078ef873b88877;
fp_table(92) -> 16#bfd2fc8977003d91;
fp_table(93) -> 16#9323372fc0536c6b;
fp_table(94) -> 16#e6316bc419a69e65;
fp_table(95) -> 16#cac0a062aef5cf9f;
fp_table(96) -> 16#3057484ea935e9e5;
fp_table(97) -> 16#1ca683e81e66b81f;
fp_table(98) -> 16#69b4df03c7934a11;
fp_table(99) -> 16#454514a570c01beb;
fp_table(100) -> 16#839066d47478ae0d;
fp_table(101) -> 16#af61ad72c32bfff7;
fp_table(102) -> 16#da73f1991ade0df9;
fp_table(103) -> 16#f6823a3fad8d5c03;
fp_table(104) -> 16#d563570e5a00291e;
fp_table(105) -> 16#f9929ca8ed5378e4;
fp_table(106) -> 16#8c80c04334a68aea;
fp_table(107) -> 16#a0710be583f5db10;
fp_table(108) -> 16#66a47994874d6ef6;
fp_table(109) -> 16#4a55b232301e3f0c;
fp_table(110) -> 16#3f47eed9e9ebcd02;
fp_table(111) -> 16#13b6257f5eb89cf8;
fp_table(112) -> 16#788534ba06f12738;
fp_table(113) -> 16#5474ff1cb1a276c2;
fp_table(114) -> 16#2166a3f7685784cc;
fp_table(115) -> 16#0d976851df04d536;
fp_table(116) -> 16#cb421a20dbbc60d0;
fp_table(117) -> 16#e7b3d1866cef312a;
fp_table(118) -> 16#92a18d6db51ac324;
fp_table(119) -> 16#be5046cb024992de;
fp_table(120) -> 16#9db12bfaf5c4e7c3;
fp_table(121) -> 16#b140e05c4297b639;
fp_table(122) -> 16#c452bcb79b624437;
fp_table(123) -> 16#e8a377112c3115cd;
fp_table(124) -> 16#2e7605602889a02b;
fp_table(125) -> 16#0287cec69fdaf1d1;
fp_table(126) -> 16#7795922d462f03df;
fp_table(127) -> 16#5b64598bf17c5225;
fp_table(128) -> 16#c15d213aa4d7a795;
fp_table(129) -> 16#edacea9c1384f66f;
fp_table(130) -> 16#98beb677ca710461;
fp_table(131) -> 16#b44f7dd17d22559b;
fp_table(132) -> 16#729a0fa0799ae07d;
fp_table(133) -> 16#5e6bc406cec9b187;
fp_table(134) -> 16#2b7998ed173c4389;
fp_table(135) -> 16#0788534ba06f1273;
fp_table(136) -> 16#24693e7a57e2676e;
fp_table(137) -> 16#0898f5dce0b13694;
fp_table(138) -> 16#7d8aa9373944c49a;
fp_table(139) -> 16#517b62918e179560;
fp_table(140) -> 16#97ae10e08aaf2086;
fp_table(141) -> 16#bb5fdb463dfc717c;
fp_table(142) -> 16#ce4d87ade4098372;
fp_table(143) -> 16#e2bc4c0b535ad288;
fp_table(144) -> 16#898f5dce0b136948;
fp_table(145) -> 16#a57e9668bc4038b2;
fp_table(146) -> 16#d06cca8365b5cabc;
fp_table(147) -> 16#fc9d0125d2e69b46;
fp_table(148) -> 16#3a487354d65e2ea0;
fp_table(149) -> 16#16b9b8f2610d7f5a;
fp_table(150) -> 16#63abe419b8f88d54;
fp_table(151) -> 16#4f5a2fbf0fabdcae;
fp_table(152) -> 16#6cbb428ef826a9b3;
fp_table(153) -> 16#404a89284f75f849;
fp_table(154) -> 16#3558d5c396800a47;
fp_table(155) -> 16#19a91e6521d35bbd;
fp_table(156) -> 16#df7c6c14256bee5b;
fp_table(157) -> 16#f38da7b29238bfa1;
fp_table(158) -> 16#869ffb594bcd4daf;
fp_table(159) -> 16#aa6e30fffc9e1c55;
fp_table(160) -> 16#50f9d8d3fb5e3a2f;
fp_table(161) -> 16#7c0813754c0d6bd5;
fp_table(162) -> 16#091a4f9e95f899db;
fp_table(163) -> 16#25eb843822abc821;
fp_table(164) -> 16#e33ef64926137dc7;
fp_table(165) -> 16#cfcf3def91402c3d;
fp_table(166) -> 16#badd610448b5de33;
fp_table(167) -> 16#962caaa2ffe68fc9;
fp_table(168) -> 16#b5cdc793086bfad4;
fp_table(169) -> 16#993c0c35bf38ab2e;
fp_table(170) -> 16#ec2e50de66cd5920;
fp_table(171) -> 16#c0df9b78d19e08da;
fp_table(172) -> 16#060ae909d526bd3c;
fp_table(173) -> 16#2afb22af6275ecc6;
fp_table(174) -> 16#5fe97e44bb801ec8;
fp_table(175) -> 16#7318b5e20cd34f32;
fp_table(176) -> 16#182ba427549af4f2;
fp_table(177) -> 16#34da6f81e3c9a508;
fp_table(178) -> 16#41c8336a3a3c5706;
fp_table(179) -> 16#6d39f8cc8d6f06fc;
fp_table(180) -> 16#abec8abd89d7b31a;
fp_table(181) -> 16#871d411b3e84e2e0;
fp_table(182) -> 16#f20f1df0e77110ee;
fp_table(183) -> 16#defed65650224114;
fp_table(184) -> 16#fd1fbb67a7af3409;
fp_table(185) -> 16#d1ee70c110fc65f3;
fp_table(186) -> 16#a4fc2c2ac90997fd;
fp_table(187) -> 16#880de78c7e5ac607;
fp_table(188) -> 16#4ed895fd7ae273e1;
fp_table(189) -> 16#62295e5bcdb1221b;
fp_table(190) -> 16#173b02b01444d015;
fp_table(191) -> 16#3bcac916a31781ef;
fp_table(192) -> 16#60ae909d526bd3ca;
fp_table(193) -> 16#4c5f5b3be5388230;
fp_table(194) -> 16#394d07d03ccd703e;
fp_table(195) -> 16#15bccc768b9e21c4;
fp_table(196) -> 16#d369be078f269422;
fp_table(197) -> 16#ff9875a13875c5d8;
fp_table(198) -> 16#8a8a294ae18037d6;
fp_table(199) -> 16#a67be2ec56d3662c;
fp_table(200) -> 16#859a8fdda15e1331;
fp_table(201) -> 16#a96b447b160d42cb;
fp_table(202) -> 16#dc791890cff8b0c5;
fp_table(203) -> 16#f088d33678abe13f;
fp_table(204) -> 16#365da1477c1354d9;
fp_table(205) -> 16#1aac6ae1cb400523;
fp_table(206) -> 16#6fbe360a12b5f72d;
fp_table(207) -> 16#434ffdaca5e6a6d7;
fp_table(208) -> 16#287cec69fdaf1d17;
fp_table(209) -> 16#048d27cf4afc4ced;
fp_table(210) -> 16#719f7b249309bee3;
fp_table(211) -> 16#5d6eb082245aef19;
fp_table(212) -> 16#9bbbc2f320e25aff;
fp_table(213) -> 16#b74a095597b10b05;
fp_table(214) -> 16#c25855be4e44f90b;
fp_table(215) -> 16#eea99e18f917a8f1;
fp_table(216) -> 16#cd48f3290e9addec;
fp_table(217) -> 16#e1b9388fb9c98c16;
fp_table(218) -> 16#94ab6464603c7e18;
fp_table(219) -> 16#b85aafc2d76f2fe2;
fp_table(220) -> 16#7e8fddb3d3d79a04;
fp_table(221) -> 16#527e16156484cbfe;
fp_table(222) -> 16#276c4afebd7139f0;
fp_table(223) -> 16#0b9d81580a22680a;
fp_table(224) -> 16#f10a69740de24e70;
fp_table(225) -> 16#ddfba2d2bab11f8a;
fp_table(226) -> 16#a8e9fe396344ed84;
fp_table(227) -> 16#8418359fd417bc7e;
fp_table(228) -> 16#42cd47eed0af0998;
fp_table(229) -> 16#6e3c8c4867fc5862;
fp_table(230) -> 16#1b2ed0a3be09aa6c;
fp_table(231) -> 16#37df1b05095afb96;
fp_table(232) -> 16#143e7634fed78e8b;
fp_table(233) -> 16#38cfbd924984df71;
fp_table(234) -> 16#4ddde17990712d7f;
fp_table(235) -> 16#612c2adf27227c85;
fp_table(236) -> 16#a7f958ae239ac963;
fp_table(237) -> 16#8b08930894c99899;
fp_table(238) -> 16#fe1acfe34d3c6a97;
fp_table(239) -> 16#d2eb0445fa6f3b6d;
fp_table(240) -> 16#b9d81580a22680ad;
fp_table(241) -> 16#9529de261575d157;
fp_table(242) -> 16#e03b82cdcc802359;
fp_table(243) -> 16#ccca496b7bd372a3;
fp_table(244) -> 16#0a1f3b1a7f6bc745;
fp_table(245) -> 16#26eef0bcc83896bf;
fp_table(246) -> 16#53fcac5711cd64b1;
fp_table(247) -> 16#7f0d67f1a69e354b;
fp_table(248) -> 16#5cec0ac051134056;
fp_table(249) -> 16#701dc166e64011ac;
fp_table(250) -> 16#050f9d8d3fb5e3a2;
fp_table(251) -> 16#29fe562b88e6b258;
fp_table(252) -> 16#ef2b245a8c5e07be;
fp_table(253) -> 16#c3daeffc3b0d5644;
fp_table(254) -> 16#b6c8b317e2f8a44a;
fp_table(255) -> 16#9a3978b155abf5b0.

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
