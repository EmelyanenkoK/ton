/*
    This file is part of TON Blockchain Library.

    TON Blockchain Library is free software: you can redistribute it and/or modify
    it under the terms of the GNU Lesser General Public License as published by
    the Free Software Foundation, either version 2 of the License, or
    (at your option) any later version.

    TON Blockchain Library is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Lesser General Public License for more details.

    You should have received a copy of the GNU Lesser General Public License
    along with TON Blockchain Library.  If not, see <http://www.gnu.org/licenses/>.

    Copyright 2017-2020 Telegram Systems LLP
*/
#include "td/utils/optional.h"

#include "vm/vm.h"
#include "vm/cp0.h"
#include "vm/dict.h"
#include "fift/utils.h"
#include "common/bigint.hpp"

#include "td/utils/base64.h"
#include "td/utils/tests.h"
#include "td/utils/ScopeGuard.h"
#include "td/utils/StringBuilder.h"

std::pair<int, std::string> run_vm(td::Ref<vm::Cell> cell) {
  vm::init_op_cp0();
  vm::DictionaryBase::get_empty_dictionary();

  class Logger : public td::LogInterface {
   public:
    void append(td::CSlice slice) override {
      res.append(slice.data(), slice.size());
    }
    std::string res;
  };
  static Logger logger;
  logger.res = "";
  td::set_log_fatal_error_callback([](td::CSlice message) { td::default_log_interface->append(logger.res); });
  vm::VmLog log{&logger, td::LogOptions::plain()};
  log.log_options.level = 4;
  log.log_options.fix_newlines = true;
  log.log_mask |= vm::VmLog::DumpStack;

  auto total_data_cells_before = vm::DataCell::get_total_data_cells();
  SCOPE_EXIT {
    auto total_data_cells_after = vm::DataCell::get_total_data_cells();
    ASSERT_EQ(total_data_cells_before, total_data_cells_after);
  };

  int n = 0;
  vm::Stack stack;
  try {
    vm::GasLimits gas_limit(1000, 1000);

    n = vm::run_vm_code(vm::load_cell_slice_ref(cell), stack, 0 /*flags*/, nullptr /*data*/, std::move(log) /*VmLog*/,
                    nullptr, &gas_limit);
  } catch (...) {
    LOG(FATAL) << "catch unhandled exception";
    n = 100;
  }
  return std::make_pair(n, logger.res);  // must be a copy
}

td::Ref<vm::Cell> to_cell(const unsigned char *buff, int bits) {
  return vm::CellBuilder().store_bits(buff, bits, 0).finalize();
}
void test_run_vm(td::Ref<vm::Cell> code, td::optional<int> ret_value = {}) {
  auto [ret1, a] = run_vm(code);
  auto [ret2, b] = run_vm(code);
  ASSERT_EQ(a, b);
  ASSERT_EQ(ret1, ret2);
  if (ret_value) {
    ASSERT_EQ(ret1, *ret_value);
  }
  REGRESSION_VERIFY(a);
}

void test_run_vm(td::Slice code_hex) {
  unsigned char buff[128];
  int bits = (int)td::bitstring::parse_bitstring_hex_literal(buff, sizeof(buff), code_hex.begin(), code_hex.end());
  CHECK(bits >= 0);
  test_run_vm(to_cell(buff, bits));
}

void test_run_vm_raw(td::Slice code64) {
  auto code = td::base64_decode(code64).move_as_ok();
  if (code.size() > 127) {
    code.resize(127);
  }
  test_run_vm(vm::CellBuilder().store_bytes(code).finalize());
}

TEST(VM, simple) {
  test_run_vm("ABCBABABABA");
}

TEST(VM, memory_leak_old) {
  test_run_vm("90787FDB3B");
}

TEST(VM, memory_leak) {
  test_run_vm("90707FDB3B");
}

TEST(VM, bug_div_short_any) {
  test_run_vm("6883FF73A98D");
}
TEST(VM, assert_pfx_dict_lookup) {
  test_run_vm("778B04216D73F43E018B04591277F473");
}

TEST(VM, assert_lookup_prefix) {
  test_run_vm("78E58B008B028B04010000016D90ED5272F43A755D77F4A8");
}

TEST(VM, assert_code_not_null) {
  test_run_vm("76ED40DE");
}

TEST(VM, bug_exec_dict_getnear) {
  test_run_vm("8B048B00006D72F47573655F6D656D6D656D8B007F");
}

TEST(VM, bug_stack_overflow) {
  test_run_vm("72A93AF8");
}

TEST(VM, assert_extract_minmax_key) {
  test_run_vm("6D6DEB21807AF49C2180EB21807AF41C");
}

TEST(VM, memory_leak_new) {
  test_run_vm("72E5ED40DB3603");
}

TEST(VM, unhandled_exception_1) {
  test_run_vm("70EDA2ED00");
}

TEST(VM, unhandled_exception_2) {
  // infinite loop now
  test_run_vm("EBEDB4");
}

TEST(VM, unhandled_exception_3) {
  // infinite loop now
  test_run_vm("EBEDC0");
}

TEST(VM, unhandled_exception_4) {
  test_run_vm("7F853EA1C8CB3E");
}

TEST(VM, unhandled_exception_5) {
  test_run_vm("738B04016D21F41476A721F49F");
}

TEST(VM, infinity_loop_1) {
  test_run_vm_raw("f3r4AJGQ6rDraIQ=");
}
TEST(VM, infinity_loop_2) {
  test_run_vm_raw("kpTt7ZLrig==");
}

TEST(VM, oom_1) {
  test_run_vm_raw("bXflX/BvDw==");
}

TEST(VM, bigint) {
  td::StringBuilder sb({}, true);

  using word_t = td::BigIntInfo::word_t;
  std::vector<word_t> numbers{1,
                              -1,
                              2,
                              -2,
                              100,
                              -100,
                              std::numeric_limits<word_t>::max(),
                              std::numeric_limits<word_t>::min(),
                              std::numeric_limits<word_t>::max() - 1,
                              std::numeric_limits<word_t>::min() + 1};
  for (auto x : numbers) {
    for (auto y : numbers) {
      word_t a;
      word_t b;
      td::BigIntInfo::set_mul(&a, &b, x, y);
      sb << "set_mul " << x << " * " << y << " = " << a << " " << b << "\n";
      td::BigIntInfo::add_mul(&a, &b, x, y);
      sb << "add_mul " << x << " " << y << " = " << a << " " << b << "\n";
      td::BigIntInfo::sub_mul(&a, &b, x, y);
      sb << "sub_mul " << x << " " << y << " = " << a << " " << b << "\n";
    }
  }
  auto base = td::BigIntInfo::Base;
  std::vector<word_t> lo_numbers{1, -1, 2, -2, 100, -100, base - 1, base - 2, -base + 1, -base + 2};
  for (auto x : numbers) {
    for (auto y : lo_numbers) {
      for (auto z : numbers) {
        word_t a;
        word_t b;
        td::BigIntInfo::dbl_divmod(&a, &b, x, y, z);
        sb << "dbl_divmod " << x << " " << y << " / " << z << " = " << a << " " << b << "\n";
      }
    }
  }

  REGRESSION_VERIFY(sb.as_cslice());
}

TEST(VM, report3_1) {
  //WA: expect (1, 2, 6, 3)
  td::Slice test1 =
      R"A(
CONT:<{
DEPTH
}>
3 SETNUMARGS
c0 POPCTR
1 INT
2 INT
3 INT
4 INT
5 INT
6 INT
4 RETURNARGS
)A";
  test_run_vm(fift::compile_asm(test1).move_as_ok());
}

TEST(VM, report3_2) {
  td::Slice test1 =
      R"A(
CONT:<{
DEPTH
}>
2 SETNUMARGS
c0 POPCTR
1 INT
2 INT
3 INT
4 INT
2 RETARGS
)A";
  test_run_vm(fift::compile_asm(test1).move_as_ok());
}

TEST(VM, report3_3) {
  // WA: expect (9)
  td::Slice test1 =
      R"A(
CONT:<{
 8 INT
}>
c0 POPCTR
CONT:<{
 9 INT
}>
c1 POPCTR
0 INT
BRANCH
)A";
  test_run_vm(fift::compile_asm(test1).move_as_ok());
}

TEST(VM, report3_4) {
  td::Slice test1 =
      R"A(
CONT:<{
1 INT
2 INT
3 INT
2 RETARGS
}>
CALLX
ADD
)A";
  test_run_vm(fift::compile_asm(test1).move_as_ok());
}

TEST(VM, report3_6) {
  // WA: expect StackOverflow
  td::Slice test1 =
      R"A(
10 INT
20 INT
30 INT
CONT:<{
  DEPTH
  40 INT
  SWAP
}>
2 SETNUMARGS
3 1 CALLXARGS
)A";
  test_run_vm(fift::compile_asm(test1).move_as_ok());
}

//TEST(VM, report3_ce) {
//td::Slice test1 =
//R"A(
//s16 POP
//s16 PUSH
//s0 s16 XCHG
//)A";
//test_run_vm(fift::compile_asm(test1).move_as_ok());
//}

TEST(VM, report3_int_overflow_1) {
  td::Slice test1 =
      R"A(
4 INT
16 INT
-115792089237316195423570985008687907853269984665640564039457584007913129639936 INT
MULDIVMOD
)A";
  test_run_vm(fift::compile_asm(test1).move_as_ok());
}
TEST(VM, report3_int_overflow_2) {
  td::Slice test1 =
      R"A(
4 INT
16 INT
-115792089237316195423570985008687907853269984665640564039457584007913129639936 INT
MULDIVR
)A";
  test_run_vm(fift::compile_asm(test1).move_as_ok());
}

TEST(VM, report3_qnot) {
  td::Slice test1 =
      R"A(
PUSHNAN
QNOT
)A";
  test_run_vm(fift::compile_asm(test1).move_as_ok());
}

TEST(VM, report3_loop_1) {
  //WA
  td::Slice test1 =
      R"A(
CONT:<{
  2 INT
}>
ATEXITALT
CONT:<{
 1 INT
 RETALT
 -1 INT
}>
AGAIN
3 INT
)A";
  test_run_vm(fift::compile_asm(test1).move_as_ok());
}

TEST(VM, report3_loop_2) {
  //WA
  td::Slice test1 =
      R"A(
CONT:<{
  2 INT
}>
ATEXITALT
CONT:<{
 1 INT
 RETALT
 -1 INT
}>
UNTIL
3 INT
)A";
  test_run_vm(fift::compile_asm(test1).move_as_ok());
}

TEST(VM, report3_loop_3) {
  //WA
  td::Slice test1 =
      R"A(
1 INT
CONT:<{
  UNTILEND
  RET
  -1 INT
}>
CALLX
)A";
  test_run_vm(fift::compile_asm(test1).move_as_ok());
}

TEST(VM, report3_loop_4) {
  //WA
  td::Slice test1 =
      R"A(
CONT:<{
  2 INT
}>
ATEXITALT
CONT:<{
  1 INT
  RETALT
  -1 PUSHINT
}>
CONT:<{
  -1 INT
}>
WHILE
3 INT
)A";
  test_run_vm(fift::compile_asm(test1).move_as_ok());
}
TEST(VM, report3_loop_5) {
  //WA
  td::Slice test1 =
      R"A(
CONT:<{
  1 INT
  2 INT
}>
ATEXITALT
3 INT
AGAINEND
DEC
DUP
IFRET
DROP
RETALT
)A";
  test_run_vm(fift::compile_asm(test1).move_as_ok());
}

TEST(VM, report3_loop_6) {
  //WA
  td::Slice test1 =
      R"A(
CONT:<{
  1 INT
  2 INT
}>
3 INT
AGAINEND
DEC
DUP
IFRET
DROP
ATEXITALT
RETALT
)A";
  test_run_vm(fift::compile_asm(test1).move_as_ok());
}

TEST(VM, super_div) {
  td::Slice test1 =
      R"A(
    303424019600764 PUSHINT 
    1000 PUSHINT
    67374462762615477834925 PUSHINT
    MULDIVR
)A";
  test_run_vm(fift::compile_asm(test1).move_as_ok(), 0);
}

void test_bigint_div(const std::string& dividend, const std::string& divisor, int round_mode, const std::string& ans_quot, const std::string& ans_remainder) {
  td::BigInt256::DoubleInt dividend_bn;
  dividend_bn.parse_dec(dividend);
  td::BigInt256::DoubleInt divisor_bn;
  divisor_bn.parse_dec(divisor);
  td::BigInt256::DoubleInt quot_bn;
  
  dividend_bn.mod_div(divisor_bn, quot_bn, round_mode);
  
  td::BigInt256::DoubleInt ans_quot_bn;
  if (ans_quot == "NaN") {
    ans_quot_bn.invalidate();
  } else {
    ans_quot_bn.parse_dec(ans_quot);
  }
  td::BigInt256::DoubleInt ans_remainder_bn;
  if (ans_remainder == "NaN") {
    ans_remainder_bn.invalidate();
  } else {
    ans_remainder_bn.parse_dec(ans_remainder);
  }
  ASSERT_EQ(dividend_bn.to_dec_string(), ans_remainder_bn.to_dec_string());
  ASSERT_EQ(quot_bn.to_dec_string(), ans_quot_bn.to_dec_string());
}

TEST(VM, super_div2) {
  test_bigint_div("1000", "9", 1, "112", "-8");
  test_bigint_div("1000000000000000000", "9000000000000000", 1, "112", "-8000000000000000");
  test_bigint_div("-1000", "9", 1, "-111", "-1");
  test_bigint_div("-1000000000000000000", "9000000000000000", 1, "-111", "-1000000000000000");
  test_bigint_div("1000", "-9", 1, "-111", "1");
  test_bigint_div("1000000000000000000", "-9000000000000000", 1, "-111", "1000000000000000");
  test_bigint_div("-1000", "-9", 1, "112", "8");
  test_bigint_div("-1000000000000000000", "-9000000000000000", 1, "112", "8000000000000000");
  
  test_bigint_div("1000", "9", 0, "111", "1");
  test_bigint_div("1000000000000000000", "9000000000000000", 0, "111", "1000000000000000");
  test_bigint_div("-1000", "9", 0, "-111", "-1");
  test_bigint_div("-1000000000000000000", "9000000000000000", 0, "-111", "-1000000000000000");
  test_bigint_div("1000", "-9", 0, "-111", "1");
  test_bigint_div("1000000000000000000", "-9000000000000000", 0, "-111", "1000000000000000");
  test_bigint_div("-1000", "-9", 0, "111", "-1");
  test_bigint_div("-1000000000000000000", "-9000000000000000", 0, "111", "-1000000000000000");
  
  test_bigint_div("5000000000000000000", "2000000000000000000", 0, "3", "-1000000000000000000");
  test_bigint_div("-5000000000000000000", "2000000000000000000", 0, "-2", "-1000000000000000000");
  test_bigint_div("5000000000000000000", "-2000000000000000000", 0, "-2", "1000000000000000000");
  test_bigint_div("-5000000000000000000", "-2000000000000000000", 0, "3", "1000000000000000000");
  
  test_bigint_div("1000", "9", -1, "111", "1");
  test_bigint_div("1000000000000000000", "9000000000000000", -1, "111", "1000000000000000");
  test_bigint_div("-1000", "9", -1, "-112", "8");
  test_bigint_div("-1000000000000000000", "9000000000000000", -1, "-112", "8000000000000000");
  test_bigint_div("1000", "-9", -1, "-112", "-8");
  test_bigint_div("1000000000000000000", "-9000000000000000", -1, "-112", "-8000000000000000");
  test_bigint_div("-1000", "-9", -1, "111", "-1");
  test_bigint_div("-1000000000000000000", "-9000000000000000", -1, "111", "-1000000000000000");
  
  test_bigint_div("303424019600764000", "67374462762615477834925", 0, "0", "303424019600764000");
  test_bigint_div("3034724019600764000", "67374462762615477834925", 0, "0", "3034724019600764000");
  test_bigint_div("30934724019600764000", "67374462762615477834925", 0, "0", "30934724019600764000");
  test_bigint_div("30934724401965080764000", "67374462762615477834925", 0, "0", "30934724401965080764000");
  test_bigint_div("309347247401965080764000", "67374462762615477834925", 0, "5", "-27525066411112308410625");
  test_bigint_div("2309347247401965080764000", "67374462762615477834925", 0, "34", "18615513473038834376550");
  test_bigint_div("23093472474019650810764000", "67374462762615477834925", 0, "343", "-15968253557458086615275");
  test_bigint_div("23093472474019650810764000", "68374492762615427834911", 0, "338", "-17106079744363797435918");
  test_bigint_div("23093472474019650810764000", "18374592262615427834517", 0, "1257", "-3390000087941977223869");
  test_bigint_div("2573300903069472664743019426508341031766400380", "1858237458809226201617542047938488403458417", 0, "1385", "-357977381305624497276309886465407023507165");
  test_bigint_div("25733009030694726647744301942650558341009131766400380", "1858237458809226201617534420474793848840393458417", 0, "13848", "136701104562207744685287915613122267363154241764");
  test_bigint_div("2530090306947266477443019426505584100931766400380", "1858237458809226201617534420474793848840393458417", 0, "1", "671852848138040275825485006030790252091372941963");
  test_bigint_div("253009030694726647743019426505584100931766400380", "1858237458809226201617534420474793848840393458417", 0, "0", "253009030694726647743019426505584100931766400380");
  test_bigint_div("25300903069472664774301942650558410093176640038", "1858237458809226201617534420474793848840393458417", 0, "0", "25300903069472664774301942650558410093176640038");
  test_bigint_div("253009030698", "1858237458809226201617534420474793848840393458417", 0, "0", "253009030698");
  test_bigint_div("19251874299181659011971842759180", "35863987523541299551572269966", 0, "537", "-7087000960018847222466212562");
  test_bigint_div("15313291547689795475496512878911", "20557862036838502455082", 0, "744887358", "8940664709053968225555");
  test_bigint_div("12381583265651812264166510594403", "98847588943634081944421359", 0, "125259", "33122161150793890235587422");
  test_bigint_div("12381583265651812264166510594403", "0", 0, "NaN", "NaN"); 
  test_bigint_div("115792089237316195423570985008687907853269984665640564039457584007913129639935", "115792089237316195423570985008687907853269984665640564039457584007913129639934", 0, "1", "1");
  test_bigint_div("156", "115792089237316195423570985008687907853269984665640564039457584007913129639934", 0, "0", "156");
  test_bigint_div("115792089237316195423570985008687907853269984665640564039457584007913129639935", "156", 0, "742256982290488432202378109030050691367115286318208743842676820563545702820", "15");
}

TEST(VM, another_div_issue) {
  td::Slice test1 =
      R"A(
    1 PUSHINT
    1 PUSHINT
    10133099161583616 PUSHINT
    MULDIVMOD
)A";
  test_run_vm(fift::compile_asm(test1).move_as_ok(), 0);
}
