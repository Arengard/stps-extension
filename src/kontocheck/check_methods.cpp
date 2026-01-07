#include "check_methods.hpp"
#include <cstring>
#include <algorithm>

namespace duckdb {
namespace stps {
namespace kontocheck {

// Weight tables for various methods
const int CheckMethods::WEIGHTS_00[9] = {2, 1, 2, 1, 2, 1, 2, 1, 2};
const int CheckMethods::WEIGHTS_01[9] = {1, 7, 3, 1, 7, 3, 1, 7, 3};
const int CheckMethods::WEIGHTS_02[9] = {2, 9, 8, 7, 6, 5, 4, 3, 2};
const int CheckMethods::WEIGHTS_04[9] = {4, 3, 2, 7, 6, 5, 4, 3, 2};
const int CheckMethods::WEIGHTS_05[9] = {1, 3, 7, 1, 3, 7, 1, 3, 7};
const int CheckMethods::WEIGHTS_06[9] = {4, 3, 2, 7, 6, 5, 4, 3, 2};
const int CheckMethods::WEIGHTS_07[9] = {10, 9, 8, 7, 6, 5, 4, 3, 2};
const int CheckMethods::WEIGHTS_10[9] = {10, 9, 8, 7, 6, 5, 4, 3, 2};
const int CheckMethods::WEIGHTS_14[6] = {7, 6, 5, 4, 3, 2};
const int CheckMethods::WEIGHTS_15[4] = {5, 4, 3, 2};
const int CheckMethods::WEIGHTS_16[9] = {4, 3, 2, 7, 6, 5, 4, 3, 2};
const int CheckMethods::WEIGHTS_17[6] = {1, 2, 1, 2, 1, 2};
const int CheckMethods::WEIGHTS_18[9] = {3, 1, 7, 9, 3, 1, 7, 9, 3};
const int CheckMethods::WEIGHTS_19[9] = {1, 9, 8, 7, 6, 5, 4, 3, 2};
const int CheckMethods::WEIGHTS_20[9] = {3, 9, 8, 7, 6, 5, 4, 3, 2};
const int CheckMethods::WEIGHTS_23[6] = {7, 6, 5, 4, 3, 2};
const int CheckMethods::WEIGHTS_24[9] = {1, 2, 3, 1, 2, 3, 1, 2, 3};
const int CheckMethods::WEIGHTS_25[8] = {9, 8, 7, 6, 5, 4, 3, 2};
const int CheckMethods::WEIGHTS_26_V1[7] = {2, 7, 6, 5, 4, 3, 2};
const int CheckMethods::WEIGHTS_26_V2[7] = {2, 7, 6, 5, 4, 3, 2};
const int CheckMethods::WEIGHTS_30[5] = {2, 1, 2, 1, 2};
const int CheckMethods::WEIGHTS_31[9] = {1, 2, 3, 4, 5, 6, 7, 8, 9};
const int CheckMethods::WEIGHTS_32[6] = {7, 6, 5, 4, 3, 2};
const int CheckMethods::WEIGHTS_33[5] = {6, 5, 4, 3, 2};
const int CheckMethods::WEIGHTS_34[7] = {7, 9, 10, 5, 8, 4, 2};
const int CheckMethods::WEIGHTS_35[9] = {10, 9, 8, 7, 6, 5, 4, 3, 2};
const int CheckMethods::WEIGHTS_36[4] = {5, 8, 4, 2};
const int CheckMethods::WEIGHTS_37[5] = {10, 5, 8, 4, 2};
const int CheckMethods::WEIGHTS_38[6] = {9, 10, 5, 8, 4, 2};
const int CheckMethods::WEIGHTS_39[7] = {7, 9, 10, 5, 8, 4, 2};
const int CheckMethods::WEIGHTS_40[9] = {6, 3, 7, 9, 10, 5, 8, 4, 2};
const int CheckMethods::WEIGHTS_42[8] = {9, 8, 7, 6, 5, 4, 3, 2};
const int CheckMethods::WEIGHTS_43[9] = {9, 8, 7, 6, 5, 4, 3, 2, 1};
const int CheckMethods::WEIGHTS_50[6] = {7, 6, 5, 4, 3, 2};
const int CheckMethods::WEIGHTS_51_A[6] = {7, 6, 5, 4, 3, 2};
const int CheckMethods::WEIGHTS_51_B[5] = {6, 5, 4, 3, 2};
const int CheckMethods::WEIGHTS_51_EX1[7] = {8, 7, 6, 5, 4, 3, 2};
const int CheckMethods::WEIGHTS_51_EX2[9] = {10, 9, 8, 7, 6, 5, 4, 3, 2};
const int CheckMethods::WEIGHTS_54[7] = {2, 7, 6, 5, 4, 3, 2};
const int CheckMethods::WEIGHTS_55[9] = {8, 7, 8, 7, 6, 5, 4, 3, 2};
const int CheckMethods::WEIGHTS_56[9] = {4, 3, 2, 7, 6, 5, 4, 3, 2};
const int CheckMethods::WEIGHTS_58[5] = {6, 5, 4, 3, 2};
const int CheckMethods::WEIGHTS_64[6] = {9, 10, 5, 8, 4, 2};
const int CheckMethods::WEIGHTS_66[6] = {7, 6, 5, 4, 3, 2};
const int CheckMethods::WEIGHTS_69[7] = {8, 7, 6, 5, 4, 3, 2};
const int CheckMethods::WEIGHTS_70_V1[9] = {4, 3, 2, 7, 6, 5, 4, 3, 2};
const int CheckMethods::WEIGHTS_70_V2[6] = {7, 6, 5, 4, 3, 2};
const int CheckMethods::WEIGHTS_70_V3[8] = {7, 6, 5, 4, 3, 2, 7, 6};
const int CheckMethods::WEIGHTS_71[6] = {6, 5, 4, 3, 2, 1};
const int CheckMethods::WEIGHTS_76[6] = {7, 6, 5, 4, 3, 2};
const int CheckMethods::WEIGHTS_77_V1[5] = {5, 4, 3, 2, 1};
const int CheckMethods::WEIGHTS_77_V2[5] = {5, 4, 3, 4, 5};
const int CheckMethods::WEIGHTS_81[6] = {7, 6, 5, 4, 3, 2};
const int CheckMethods::WEIGHTS_83_A[8] = {2, 3, 4, 5, 6, 7, 8, 9};
const int CheckMethods::WEIGHTS_83_B[8] = {2, 3, 4, 5, 6, 7, 8, 9};
const int CheckMethods::WEIGHTS_83_C[8] = {2, 3, 4, 5, 6, 7, 8, 9};
const int CheckMethods::WEIGHTS_84_V1[6] = {7, 6, 5, 4, 3, 2};
const int CheckMethods::WEIGHTS_85_A[8] = {2, 3, 4, 5, 6, 7, 8, 9};
const int CheckMethods::WEIGHTS_85_B[8] = {2, 3, 4, 5, 6, 7, 8, 9};
const int CheckMethods::WEIGHTS_85_C[8] = {2, 3, 4, 5, 6, 7, 8, 9};
const int CheckMethods::WEIGHTS_86_V1[9] = {2, 1, 2, 1, 2, 1, 2, 1, 2};
const int CheckMethods::WEIGHTS_86_V2[9] = {4, 3, 2, 7, 6, 5, 4, 3, 2};
const int CheckMethods::WEIGHTS_88[9] = {2, 3, 4, 5, 6, 7, 8, 9, 10};
const int CheckMethods::WEIGHTS_90_SACH[7] = {8, 7, 6, 5, 4, 3, 2};
const int CheckMethods::WEIGHTS_90_A[6] = {7, 6, 5, 4, 3, 2};
const int CheckMethods::WEIGHTS_90_B[5] = {6, 5, 4, 3, 2};
const int CheckMethods::WEIGHTS_90_E[5] = {2, 1, 2, 1, 2};
const int CheckMethods::WEIGHTS_91_A[6] = {7, 6, 5, 4, 3, 2};
const int CheckMethods::WEIGHTS_91_B[6] = {2, 3, 4, 5, 6, 7};
const int CheckMethods::WEIGHTS_91_C[9] = {10, 9, 8, 7, 6, 5, 4, 3, 2};
const int CheckMethods::WEIGHTS_91_D[6] = {9, 10, 5, 8, 4, 2};
const int CheckMethods::WEIGHTS_92[6] = {1, 7, 3, 1, 7, 3};
const int CheckMethods::WEIGHTS_93[5] = {6, 5, 4, 3, 2};
const int CheckMethods::WEIGHTS_95[9] = {4, 3, 2, 7, 6, 5, 4, 3, 2};
const int CheckMethods::WEIGHTS_98_A[7] = {3, 7, 1, 3, 7, 1, 3};
const int CheckMethods::WEIGHTS_A0[5] = {10, 5, 8, 4, 2};
const int CheckMethods::WEIGHTS_A6[9] = {1, 7, 3, 1, 7, 3, 1, 7, 3};
const int CheckMethods::WEIGHTS_A8_V1[6] = {7, 6, 5, 4, 3, 2};
const int CheckMethods::WEIGHTS_B9_V2[6] = {6, 5, 4, 3, 2, 1};

// M10H transformation table (iterative transformation for certain methods)
const int CheckMethods::M10H_DIGITS[4][10] = {
    {0, 1, 5, 9, 3, 7, 4, 8, 2, 6},  // Row 0
    {0, 1, 7, 6, 9, 8, 3, 2, 5, 4},  // Row 1
    {0, 1, 8, 4, 6, 2, 9, 5, 7, 3},  // Row 2
    {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}   // Row 3
};

// Main dispatcher
CheckResult CheckMethods::ValidateAccount(
    const std::string& account,
    uint8_t method_id,
    const std::string& blz) {

    // Ensure account is 10 digits (pad with leading zeros if needed)
    std::string acct = account;
    if (acct.length() < 10) {
        acct = std::string(10 - acct.length(), '0') + acct;
    }
    if (acct.length() != 10) {
        return CheckResult::INVALID_KTO;
    }

    // Dispatch to specific method
    switch (method_id) {
        case 0x00: return Method_00(acct, blz);
        case 0x01: return Method_01(acct, blz);
        case 0x02: return Method_02(acct, blz);
        case 0x03: return Method_03(acct, blz);
        case 0x04: return Method_04(acct, blz);
        case 0x05: return Method_05(acct, blz);
        case 0x06: return Method_06(acct, blz);
        case 0x07: return Method_07(acct, blz);
        case 0x08: return Method_08(acct, blz);
        case 0x09: return Method_09(acct, blz);
        case 0x0A: return Method_10(acct, blz);
        case 0x0B: return Method_11(acct, blz);
        case 0x0C: return Method_12(acct, blz);
        case 0x0D: return Method_13(acct, blz);
        case 0x0E: return Method_14(acct, blz);
        case 0x0F: return Method_15(acct, blz);
        case 0x10: return Method_16(acct, blz);
        case 0x11: return Method_17(acct, blz);
        case 0x12: return Method_18(acct, blz);
        case 0x13: return Method_19(acct, blz);
        case 0x14: return Method_20(acct, blz);
        case 0x15: return Method_21(acct, blz);
        case 0x16: return Method_22(acct, blz);
        case 0x17: return Method_23(acct, blz);
        case 0x18: return Method_24(acct, blz);
        case 0x19: return Method_25(acct, blz);
        case 0x1A: return Method_26(acct, blz);
        case 0x1B: return Method_27(acct, blz);
        case 0x1C: return Method_28(acct, blz);
        case 0x1D: return Method_29(acct, blz);
        case 0x1E: return Method_30(acct, blz);
        case 0x1F: return Method_31(acct, blz);
        case 0x20: return Method_32(acct, blz);
        case 0x21: return Method_33(acct, blz);
        case 0x22: return Method_34(acct, blz);
        case 0x23: return Method_35(acct, blz);
        case 0x24: return Method_36(acct, blz);
        case 0x25: return Method_37(acct, blz);
        case 0x26: return Method_38(acct, blz);
        case 0x27: return Method_39(acct, blz);
        case 0x28: return Method_40(acct, blz);
        case 0x29: return Method_41(acct, blz);
        case 0x2A: return Method_42(acct, blz);
        case 0x2B: return Method_43(acct, blz);
        case 0x2C: return Method_44(acct, blz);
        case 0x2D: return Method_45(acct, blz);
        case 0x2E: return Method_46(acct, blz);
        case 0x2F: return Method_47(acct, blz);
        case 0x30: return Method_48(acct, blz);
        case 0x31: return Method_49(acct, blz);
        case 0x32: return Method_50(acct, blz);
        case 0x33: return Method_51(acct, blz);
        case 0x34: return Method_52(acct, blz);
        case 0x35: return Method_53(acct, blz);
        case 0x36: return Method_54(acct, blz);
        case 0x37: return Method_55(acct, blz);
        case 0x38: return Method_56(acct, blz);
        case 0x39: return Method_57(acct, blz);
        case 0x3A: return Method_58(acct, blz);
        case 0x3B: return Method_59(acct, blz);
        case 0x3C: return Method_60(acct, blz);
        case 0x3D: return Method_61(acct, blz);
        case 0x3E: return Method_62(acct, blz);
        case 0x3F: return Method_63(acct, blz);
        case 0x40: return Method_64(acct, blz);
        case 0x41: return Method_65(acct, blz);
        case 0x42: return Method_66(acct, blz);
        case 0x43: return Method_67(acct, blz);
        case 0x44: return Method_68(acct, blz);
        case 0x45: return Method_69(acct, blz);
        case 0x46: return Method_70(acct, blz);
        case 0x47: return Method_71(acct, blz);
        case 0x48: return Method_72(acct, blz);
        case 0x49: return Method_73(acct, blz);
        case 0x4A: return Method_74(acct, blz);
        case 0x4B: return Method_75(acct, blz);
        case 0x4C: return Method_76(acct, blz);
        case 0x4D: return Method_77(acct, blz);
        case 0x4E: return Method_78(acct, blz);
        case 0x4F: return Method_79(acct, blz);
        case 0x50: return Method_80(acct, blz);
        case 0x51: return Method_81(acct, blz);
        case 0x52: return Method_82(acct, blz);
        case 0x53: return Method_83(acct, blz);
        case 0x54: return Method_84(acct, blz);
        case 0x55: return Method_85(acct, blz);
        case 0x56: return Method_86(acct, blz);
        case 0x57: return Method_87(acct, blz);
        case 0x58: return Method_88(acct, blz);
        case 0x59: return Method_89(acct, blz);
        case 0x5A: return Method_90(acct, blz);
        case 0x5B: return Method_91(acct, blz);
        case 0x5C: return Method_92(acct, blz);
        case 0x5D: return Method_93(acct, blz);
        case 0x5E: return Method_94(acct, blz);
        case 0x5F: return Method_95(acct, blz);
        case 0x60: return Method_96(acct, blz);
        case 0x61: return Method_97(acct, blz);
        case 0x62: return Method_98(acct, blz);
        case 0x63: return Method_99(acct, blz);
        case 0xA0: return Method_A0(acct, blz);
        case 0xA1: return Method_A1(acct, blz);
        case 0xA2: return Method_A2(acct, blz);
        case 0xA3: return Method_A3(acct, blz);
        case 0xA4: return Method_A4(acct, blz);
        case 0xA5: return Method_A5(acct, blz);
        case 0xA6: return Method_A6(acct, blz);
        case 0xA7: return Method_A7(acct, blz);
        case 0xA8: return Method_A8(acct, blz);
        case 0xA9: return Method_A9(acct, blz);
        case 0xB0: return Method_B0(acct, blz);
        case 0xB1: return Method_B1(acct, blz);
        case 0xB2: return Method_B2(acct, blz);
        case 0xB3: return Method_B3(acct, blz);
        case 0xB4: return Method_B4(acct, blz);
        case 0xB5: return Method_B5(acct, blz);
        case 0xB6: return Method_B6(acct, blz);
        case 0xB7: return Method_B7(acct, blz);
        case 0xB8: return Method_B8(acct, blz);
        case 0xB9: return Method_B9(acct, blz);
        case 0xC0: return Method_C0(acct, blz);
        case 0xC1: return Method_C1(acct, blz);
        case 0xC2: return Method_C2(acct, blz);
        case 0xC3: return Method_C3(acct, blz);
        case 0xC4: return Method_C4(acct, blz);
        case 0xC5: return Method_C5(acct, blz);
        case 0xC6: return Method_C6(acct, blz);
        // More cases will be added for remaining methods
        default:
            return CheckResult::NOT_IMPLEMENTED;
    }
}

// ======================================================================
// Method 00: Modulus 10, Gewichtung 2, 1, 2, 1, 2, 1, 2, 1, 2
// ======================================================================
CheckResult CheckMethods::Method_00(const std::string& account, const std::string& blz) {
    int sum = 0;

    // Apply weights and calculate cross-sum for products >= 10
    for (int i = 0; i < 9; i++) {
        int digit = account[i] - '0';
        int weighted = digit * WEIGHTS_00[i];

        // Cross-sum (Quersumme): e.g., 16 becomes 1+6=7
        if (weighted >= 10) {
            sum += (weighted / 10) + (weighted % 10);
        } else {
            sum += weighted;
        }
    }

    // Calculate check digit
    int check_digit = (10 - (sum % 10)) % 10;
    int expected = account[9] - '0';

    return (check_digit == expected) ? CheckResult::OK : CheckResult::FALSE;
}

// ======================================================================
// Method 01: Modulus 10, Gewichtung 3, 7, 1, 3, 7, 1, 3, 7, 1
// ======================================================================
CheckResult CheckMethods::Method_01(const std::string& account, const std::string& blz) {
    int sum = 0;

    for (int i = 0; i < 9; i++) {
        sum += (account[i] - '0') * WEIGHTS_01[i];
    }

    int check_digit = (10 - (sum % 10)) % 10;
    int expected = account[9] - '0';

    return (check_digit == expected) ? CheckResult::OK : CheckResult::FALSE;
}

// ======================================================================
// Method 02: Modulus 11, Gewichtung 2, 3, 4, 5, 6, 7, 8, 9, 2
// ======================================================================
CheckResult CheckMethods::Method_02(const std::string& account, const std::string& blz) {
    int sum = 0;

    for (int i = 0; i < 9; i++) {
        sum += (account[i] - '0') * WEIGHTS_02[i];
    }

    int remainder = sum % 11;
    int check_digit = (remainder == 0) ? 0 : (11 - remainder);

    // If check digit is 10, account is invalid
    if (check_digit == 10) {
        return CheckResult::INVALID_KTO;
    }

    int expected = account[9] - '0';
    return (check_digit == expected) ? CheckResult::OK : CheckResult::FALSE;
}

// ======================================================================
// Method 03: Modulus 10, Gewichtung 2, 1, 2, 1, 2, 1, 2, 1, 2
// Same as Method 00 (alternative specification name)
// ======================================================================
CheckResult CheckMethods::Method_03(const std::string& account, const std::string& blz) {
    return Method_00(account, blz);
}

// ======================================================================
// Method 04: Modulus 11, Gewichtung 2, 3, 4, 5, 6, 7, 2, 3, 4
// ======================================================================
CheckResult CheckMethods::Method_04(const std::string& account, const std::string& blz) {
    int sum = 0;

    for (int i = 0; i < 9; i++) {
        sum += (account[i] - '0') * WEIGHTS_04[i];
    }

    int remainder = sum % 11;
    int check_digit = (remainder == 0) ? 0 : (11 - remainder);

    if (check_digit == 10) {
        return CheckResult::INVALID_KTO;
    }

    int expected = account[9] - '0';
    return (check_digit == expected) ? CheckResult::OK : CheckResult::FALSE;
}

// ======================================================================
// Method 05: Modulus 10, Gewichtung 7, 3, 1, 7, 3, 1, 7, 3, 1
// ======================================================================
CheckResult CheckMethods::Method_05(const std::string& account, const std::string& blz) {
    int sum = 0;

    for (int i = 0; i < 9; i++) {
        sum += (account[i] - '0') * WEIGHTS_05[i];
    }

    int check_digit = (10 - (sum % 10)) % 10;
    int expected = account[9] - '0';

    return (check_digit == expected) ? CheckResult::OK : CheckResult::FALSE;
}

// ======================================================================
// Method 06: Modulus 11, Gewichtung 2, 3, 4, 5, 6, 7 (modifiziert)
// Special: remainder 1 => check digit 0, remainder 0 => check digit 0
// ======================================================================
CheckResult CheckMethods::Method_06(const std::string& account, const std::string& blz) {
    int sum = 0;

    for (int i = 0; i < 9; i++) {
        sum += (account[i] - '0') * WEIGHTS_06[i];
    }

    int remainder = sum % 11;
    int check_digit;

    if (remainder <= 1) {
        check_digit = 0;
    } else {
        check_digit = 11 - remainder;
    }

    int expected = account[9] - '0';
    return (check_digit == expected) ? CheckResult::OK : CheckResult::FALSE;
}

// ======================================================================
// Method 07: Modulus 11, Gewichtung 2, 3, 4, 5, 6, 7, 8, 9, 10
// ======================================================================
CheckResult CheckMethods::Method_07(const std::string& account, const std::string& blz) {
    int sum = 0;

    for (int i = 0; i < 9; i++) {
        sum += (account[i] - '0') * WEIGHTS_07[i];
    }

    int remainder = sum % 11;
    int check_digit = (remainder == 0) ? 0 : (11 - remainder);

    if (check_digit == 10) {
        return CheckResult::INVALID_KTO;
    }

    int expected = account[9] - '0';
    return (check_digit == expected) ? CheckResult::OK : CheckResult::FALSE;
}

// ======================================================================
// Method 08: Modulus 10, weight 2,1,2,1,2,1,2,1,2 (like 00)
// Only for account >= 60000
// ======================================================================
CheckResult CheckMethods::Method_08(const std::string& account, const std::string& blz) {
    // Check if account number is at least 60000
    if (std::strcmp(account.c_str(), "0000060000") < 0) {
        // No check digit validation for accounts < 60000
        return CheckResult::OK;
    }

    // Use Method 00 for accounts >= 60000
    return Method_00(account, blz);
}

// ======================================================================
// Method 09: No check digit calculation (always valid)
// ======================================================================
CheckResult CheckMethods::Method_09(const std::string& account, const std::string& blz) {
    return CheckResult::OK;
}

// ======================================================================
// Method 10: Modulus 11, Gewichtung 2,3,4,5,6,7,8,9,10 (modified like 06)
// ======================================================================
CheckResult CheckMethods::Method_10(const std::string& account, const std::string& blz) {
    int sum = 0;

    for (int i = 0; i < 9; i++) {
        sum += (account[i] - '0') * WEIGHTS_10[i];
    }

    int remainder = sum % 11;
    int check_digit;

    // Modified: remainder 0 or 1 => check digit 0
    if (remainder <= 1) {
        check_digit = 0;
    } else {
        check_digit = 11 - remainder;
    }

    int expected = account[9] - '0';
    return (check_digit == expected) ? CheckResult::OK : CheckResult::FALSE;
}

// ======================================================================
// Method 11: Modulus 11, Gewichtung 2,3,4,5,6,7,8,9,10 (modified)
// Special: remainder 1 => check digit 9 (not 0)
// ======================================================================
CheckResult CheckMethods::Method_11(const std::string& account, const std::string& blz) {
    int sum = 0;

    for (int i = 0; i < 9; i++) {
        sum += (account[i] - '0') * WEIGHTS_10[i];
    }

    int remainder = sum % 11;
    int check_digit;

    if (remainder == 1) {
        check_digit = 9;  // Special case
    } else if (remainder == 0) {
        check_digit = 0;
    } else {
        check_digit = 11 - remainder;
    }

    int expected = account[9] - '0';
    return (check_digit == expected) ? CheckResult::OK : CheckResult::FALSE;
}

// ======================================================================
// Method 12: Not defined (reserved/unused)
// ======================================================================
CheckResult CheckMethods::Method_12(const std::string& account, const std::string& blz) {
    // Method 12 is explicitly not defined in Bundesbank specification
    return CheckResult::INVALID_KTO;
}

// ======================================================================
// Method 13: Modulus 10, Gewichtung 2,1,2,1,2,1
// Special: 6-digit base number at positions 2-7, check digit at position 8
// Two-digit sub-account at positions 9-10 (not included in calculation)
// Try variant 1 first, then variant 2 if it fails
// ======================================================================
CheckResult CheckMethods::Method_13(const std::string& account, const std::string& blz) {
    // Variant 1: Check positions 1-7, check digit at position 8
    int sum = 0;
    int weights[] = {1, 2, 1, 2, 1, 2};

    for (int i = 0; i < 6; i++) {
        int digit = account[i + 1] - '0';  // Start from position 1 (index 1)
        int weighted = digit * weights[i];

        // Cross-sum for products >= 10
        if (weighted >= 10) {
            sum += (weighted / 10) + (weighted % 10);
        } else {
            sum += weighted;
        }
    }

    int check_digit_v1 = (10 - (sum % 10)) % 10;
    int expected_v1 = account[7] - '0';  // Check digit at position 8 (index 7)

    if (check_digit_v1 == expected_v1) {
        return CheckResult::OK;
    }

    // Variant 2: Check positions 3-8, check digit at position 10
    sum = 0;
    for (int i = 0; i < 6; i++) {
        int digit = account[i + 3] - '0';  // Start from position 3 (index 3)
        int weighted = digit * weights[i];

        // Cross-sum for products >= 10
        if (weighted >= 10) {
            sum += (weighted / 10) + (weighted % 10);
        } else {
            sum += weighted;
        }
    }

    int check_digit_v2 = (10 - (sum % 10)) % 10;
    int expected_v2 = account[9] - '0';  // Check digit at position 10 (index 9)

    return (check_digit_v2 == expected_v2) ? CheckResult::OK : CheckResult::FALSE;
}

// ======================================================================
// Method 14: Modulus 11, Gewichtung 2,3,4,5,6,7
// Positions 4-9 (base number), check digit at position 10
// Positions 2-3 are account type (not included)
// ======================================================================
CheckResult CheckMethods::Method_14(const std::string& account, const std::string& blz) {
    int sum = 0;

    for (int i = 0; i < 6; i++) {
        sum += (account[i + 3] - '0') * WEIGHTS_14[i];  // Start from position 4 (index 3)
    }

    int remainder = sum % 11;
    int check_digit = (remainder == 0) ? 0 : (11 - remainder);

    if (check_digit == 10) {
        return CheckResult::INVALID_KTO;
    }

    int expected = account[9] - '0';
    return (check_digit == expected) ? CheckResult::OK : CheckResult::FALSE;
}

// ======================================================================
// Method 15: Modulus 11, Gewichtung 2,3,4,5
// Only 4-digit account number at positions 6-9
// Check digit at position 10
// ======================================================================
CheckResult CheckMethods::Method_15(const std::string& account, const std::string& blz) {
    int sum = 0;

    for (int i = 0; i < 4; i++) {
        sum += (account[i + 5] - '0') * WEIGHTS_15[i];  // Start from position 6 (index 5)
    }

    int remainder = sum % 11;
    int check_digit;

    // Modified like method 06: remainder 0 or 1 => check digit 0
    if (remainder <= 1) {
        check_digit = 0;
    } else {
        check_digit = 11 - remainder;
    }

    int expected = account[9] - '0';
    return (check_digit == expected) ? CheckResult::OK : CheckResult::FALSE;
}

// ======================================================================
// Method 16: Modulus 11, Gewichtung 2,3,4,5,6,7,2,3,4
// Special: If remainder is 1 and digits at positions 9 and 10 are identical,
// the account is valid regardless of check digit calculation
// ======================================================================
CheckResult CheckMethods::Method_16(const std::string& account, const std::string& blz) {
    int sum = 0;

    for (int i = 0; i < 9; i++) {
        sum += (account[i] - '0') * WEIGHTS_16[i];
    }

    int remainder = sum % 11;
    int check_digit = (remainder == 0) ? 0 : (11 - remainder);

    if (check_digit == 10) {
        // Special case: if digits at position 9 and 10 are identical, it's valid
        if (account[8] == account[9]) {
            return CheckResult::OK;
        } else {
            check_digit = 0;  // Otherwise use 0
        }
    }

    int expected = account[9] - '0';
    return (check_digit == expected) ? CheckResult::OK : CheckResult::FALSE;
}

// ======================================================================
// Method 17: Modulus 11, Gewichtung 1,2,1,2,1,2
// 6-digit base number (Stammnummer) at positions 2-7
// Check digit at position 8
// Subtract 1 from sum before modulus
// ======================================================================
CheckResult CheckMethods::Method_17(const std::string& account, const std::string& blz) {
    int sum = 0;

    for (int i = 0; i < 6; i++) {
        int digit = account[i + 1] - '0';  // Positions 2-7 (index 1-6)
        int weighted = digit * WEIGHTS_17[i];

        // Cross-sum for products >= 10
        if (weighted >= 10) {
            sum += (weighted / 10) + (weighted % 10);
        } else {
            sum += weighted;
        }
    }

    // Subtract 1 before modulus
    sum -= 1;

    int remainder = sum % 11;
    int check_digit = (remainder == 0) ? 0 : (10 - remainder);

    int expected = account[7] - '0';  // Check digit at position 8 (index 7)
    return (check_digit == expected) ? CheckResult::OK : CheckResult::FALSE;
}

// ======================================================================
// Method 18: Modulus 10, Gewichtung 3,9,7,1,3,9,7,1,3
// ======================================================================
CheckResult CheckMethods::Method_18(const std::string& account, const std::string& blz) {
    int sum = 0;

    for (int i = 0; i < 9; i++) {
        sum += (account[i] - '0') * WEIGHTS_18[i];
    }

    int check_digit = (10 - (sum % 10)) % 10;
    int expected = account[9] - '0';

    return (check_digit == expected) ? CheckResult::OK : CheckResult::FALSE;
}

// ======================================================================
// Method 19: Modulus 11, Gewichtung 2,3,4,5,6,7,8,9,1 (modified like 06)
// ======================================================================
CheckResult CheckMethods::Method_19(const std::string& account, const std::string& blz) {
    int sum = 0;

    for (int i = 0; i < 9; i++) {
        sum += (account[i] - '0') * WEIGHTS_19[i];
    }

    int remainder = sum % 11;
    int check_digit;

    // Modified like method 06: remainder 0 or 1 => check digit 0
    if (remainder <= 1) {
        check_digit = 0;
    } else {
        check_digit = 11 - remainder;
    }

    int expected = account[9] - '0';
    return (check_digit == expected) ? CheckResult::OK : CheckResult::FALSE;
}

// ======================================================================
// Method 20: Modulus 11, Gewichtung 3,9,8,7,6,5,4,3,2 (modified like 06)
// ======================================================================
CheckResult CheckMethods::Method_20(const std::string& account, const std::string& blz) {
    int sum = 0;

    for (int i = 0; i < 9; i++) {
        sum += (account[i] - '0') * WEIGHTS_20[i];
    }

    int remainder = sum % 11;
    int check_digit;

    // Modified like method 06: remainder 0 or 1 => check digit 0
    if (remainder <= 1) {
        check_digit = 0;
    } else {
        check_digit = 11 - remainder;
    }

    int expected = account[9] - '0';
    return (check_digit == expected) ? CheckResult::OK : CheckResult::FALSE;
}

// ======================================================================
// Method 21: Modulus 10, Gewichtung 2,1,2,1,2,1,2,1,2 (modified)
// Special: Iterative cross-sum until single digit
// ======================================================================
CheckResult CheckMethods::Method_21(const std::string& account, const std::string& blz) {
    int sum = 0;

    // Calculate weighted sum without cross-sum
    for (int i = 0; i < 9; i++) {
        int digit = account[i] - '0';
        int weight = (i % 2 == 0) ? 2 : 1;
        sum += digit * weight;
    }

    // Iterative cross-sum until single digit
    while (sum >= 10) {
        int cross_sum = 0;
        while (sum > 0) {
            cross_sum += sum % 10;
            sum /= 10;
        }
        sum = cross_sum;
    }

    int check_digit = (10 - sum) % 10;
    int expected = account[9] - '0';

    return (check_digit == expected) ? CheckResult::OK : CheckResult::FALSE;
}

// ======================================================================
// Method 22: Modulus 10, Gewichtung 3,1,3,1,3,1,3,1,3
// Special: Tens digits are discarded from products
// ======================================================================
CheckResult CheckMethods::Method_22(const std::string& account, const std::string& blz) {
    int sum = 0;

    for (int i = 0; i < 9; i++) {
        int digit = account[i] - '0';
        int weight = (i % 2 == 0) ? 3 : 1;
        int product = digit * weight;
        // Only use ones digit (discard tens)
        sum += product % 10;
    }

    int check_digit = (10 - (sum % 10)) % 10;
    int expected = account[9] - '0';

    return (check_digit == expected) ? CheckResult::OK : CheckResult::FALSE;
}

// ======================================================================
// Method 23: Modulus 11, Gewichtung 2,3,4,5,6,7
// First 6 digits only, check digit at position 7
// Special: If remainder is 1 and digits 6 and 7 match, valid
// ======================================================================
CheckResult CheckMethods::Method_23(const std::string& account, const std::string& blz) {
    int sum = 0;

    for (int i = 0; i < 6; i++) {
        sum += (account[i] - '0') * WEIGHTS_23[i];
    }

    int remainder = sum % 11;
    int check_digit = (remainder == 0) ? 0 : (11 - remainder);

    if (check_digit == 10) {
        // Special case: if digits at position 6 and 7 are identical, it's valid
        if (account[5] == account[6]) {
            return CheckResult::OK;
        } else {
            return CheckResult::INVALID_KTO;
        }
    }

    int expected = account[6] - '0';  // Check digit at position 7 (index 6)
    return (check_digit == expected) ? CheckResult::OK : CheckResult::FALSE;
}

// ======================================================================
// Method 24: Modulus 11/10, Gewichtung 1,2,3,1,2,3,1,2,3
// Complex: digit substitution, skip leading zeros, special calculation
// ======================================================================
CheckResult CheckMethods::Method_24(const std::string& account, const std::string& blz) {
    std::string acct = account;

    // Digit 3,4,5,6 at position 1 => treat as 0
    if (acct[0] >= '3' && acct[0] <= '6') {
        acct[0] = '0';
    }

    // Digit 9 at position 1 => positions 1,2,3 become 0
    if (acct[0] == '9') {
        acct[0] = acct[1] = acct[2] = '0';
    }

    // Find first non-zero digit
    size_t start = 0;
    while (start < 9 && acct[start] == '0') {
        start++;
    }

    // Calculate: multiply by weight, add weight, then MOD 11
    int sum = 0;
    for (size_t i = start; i < 9; i++) {
        int digit = acct[i] - '0';
        int weight_idx = (i - start) % 3;
        int weight = WEIGHTS_24[weight_idx];
        int product = (digit * weight) + weight;
        sum += product % 11;
    }

    int check_digit = sum % 10;
    int expected = account[9] - '0';

    return (check_digit == expected) ? CheckResult::OK : CheckResult::FALSE;
}

// ======================================================================
// Method 25: Modulus 11, Gewichtung 9,8,7,6,5,4,3,2
// Positions 2-9, check digit at position 10
// Special: If remainder is 1, check digit is 0 only for work digits 8,9
// ======================================================================
CheckResult CheckMethods::Method_25(const std::string& account, const std::string& blz) {
    int sum = 0;

    for (int i = 0; i < 8; i++) {
        sum += (account[i + 1] - '0') * WEIGHTS_25[i];  // Positions 2-9 (index 1-8)
    }

    int remainder = sum % 11;
    int check_digit = (remainder == 0) ? 0 : (11 - remainder);

    if (check_digit == 10) {
        check_digit = 0;
        // Work digit is at position 2 (index 1)
        char work_digit = account[1];
        if (work_digit != '8' && work_digit != '9') {
            return CheckResult::INVALID_KTO;
        }
    }

    int expected = account[9] - '0';
    return (check_digit == expected) ? CheckResult::OK : CheckResult::FALSE;
}

// ======================================================================
// Method 26: Modulus 11, Gewichtung 2,7,6,5,4,3,2
// Special: If positions 1-2 are "00", shift account left by 2
// Check positions 1-7 or 3-9 depending on shift
// ======================================================================
CheckResult CheckMethods::Method_26(const std::string& account, const std::string& blz) {
    if (account[0] == '0' && account[1] == '0') {
        // Shifted: check positions 3-9, check digit at position 10
        int sum = 0;
        for (int i = 0; i < 7; i++) {
            sum += (account[i + 2] - '0') * WEIGHTS_26_V1[i];  // Positions 3-9 (index 2-8)
        }

        int remainder = sum % 11;
        int check_digit;

        if (remainder <= 1) {
            check_digit = 0;
        } else {
            check_digit = 11 - remainder;
        }

        int expected = account[9] - '0';
        return (check_digit == expected) ? CheckResult::OK : CheckResult::FALSE;
    } else {
        // Normal: check positions 1-7, check digit at position 8
        int sum = 0;
        for (int i = 0; i < 7; i++) {
            sum += (account[i] - '0') * WEIGHTS_26_V2[i];  // Positions 1-7 (index 0-6)
        }

        int remainder = sum % 11;
        int check_digit;

        if (remainder <= 1) {
            check_digit = 0;
        } else {
            check_digit = 11 - remainder;
        }

        int expected = account[7] - '0';  // Check digit at position 8 (index 7)
        return (check_digit == expected) ? CheckResult::OK : CheckResult::FALSE;
    }
}

// ======================================================================
// Method 27: Modulus 10 for accounts < 1000000000, M10H transformation for >= 1000000000
// M10H uses transformation table with 4 rows
// ======================================================================
CheckResult CheckMethods::Method_27(const std::string& account, const std::string& blz) {
    // Transformation table for M10H (iterated transformation)
    static const int m10h_table[4][10] = {
        {0, 1, 5, 9, 3, 7, 4, 8, 2, 6},  // Row 1
        {0, 1, 7, 6, 9, 8, 3, 2, 5, 4},  // Row 2
        {0, 1, 8, 4, 6, 2, 9, 5, 7, 3},  // Row 3
        {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}   // Row 4
    };

    if (account[0] == '0') {
        // Accounts 1-999999999: Use Method 00
        return Method_00(account, blz);
    } else {
        // Accounts >= 1000000000: Use M10H transformation
        int sum = 0;
        int row_pattern[] = {0, 3, 2, 1, 0, 3, 2, 1, 0};  // Row indices for positions 1-9

        for (int i = 0; i < 9; i++) {
            int digit = account[i] - '0';
            sum += m10h_table[row_pattern[i]][digit];
        }

        int check_digit = (10 - (sum % 10)) % 10;
        int expected = account[9] - '0';

        return (check_digit == expected) ? CheckResult::OK : CheckResult::FALSE;
    }
}

// ======================================================================
// Method 28: Modulus 11, Gewichtung 8,7,6,5,4,3,2
// Positions 1-7, check digit at position 8
// Modified like method 06
// ======================================================================
CheckResult CheckMethods::Method_28(const std::string& account, const std::string& blz) {
    static const int weights[] = {8, 7, 6, 5, 4, 3, 2};
    int sum = 0;

    for (int i = 0; i < 7; i++) {
        sum += (account[i] - '0') * weights[i];
    }

    int remainder = sum % 11;
    int check_digit;

    if (remainder <= 1) {
        check_digit = 0;
    } else {
        check_digit = 11 - remainder;
    }

    int expected = account[7] - '0';  // Check digit at position 8 (index 7)
    return (check_digit == expected) ? CheckResult::OK : CheckResult::FALSE;
}

// ======================================================================
// Method 29: Modulus 10, Iterierte Transformation (M10H)
// All accounts use M10H transformation table
// ======================================================================
CheckResult CheckMethods::Method_29(const std::string& account, const std::string& blz) {
    // Transformation table for M10H
    static const int m10h_table[4][10] = {
        {0, 1, 5, 9, 3, 7, 4, 8, 2, 6},  // Row 1
        {0, 1, 7, 6, 9, 8, 3, 2, 5, 4},  // Row 2
        {0, 1, 8, 4, 6, 2, 9, 5, 7, 3},  // Row 3
        {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}   // Row 4
    };

    int sum = 0;
    int row_pattern[] = {0, 3, 2, 1, 0, 3, 2, 1, 0};  // Row indices for positions 1-9

    for (int i = 0; i < 9; i++) {
        int digit = account[i] - '0';
        sum += m10h_table[row_pattern[i]][digit];
    }

    int check_digit = (10 - (sum % 10)) % 10;
    int expected = account[9] - '0';

    return (check_digit == expected) ? CheckResult::OK : CheckResult::FALSE;
}

// ======================================================================
// Method 30: Modulus 10, Gewichtung 2,0,0,0,0,1,2,1,2
// Only positions 1, 6, 7, 8, 9 are weighted
// ======================================================================
CheckResult CheckMethods::Method_30(const std::string& account, const std::string& blz) {
    int sum = 0;

    sum += (account[0] - '0') * 2;  // Position 1
    sum += (account[5] - '0') * 1;  // Position 6
    sum += (account[6] - '0') * 2;  // Position 7
    sum += (account[7] - '0') * 1;  // Position 8
    sum += (account[8] - '0') * 2;  // Position 9

    int check_digit = (10 - (sum % 10)) % 10;
    int expected = account[9] - '0';

    return (check_digit == expected) ? CheckResult::OK : CheckResult::FALSE;
}

// ======================================================================
// Method 31: Modulus 11, Gewichtung 9,8,7,6,5,4,3,2,1
// Positions 1-9, check digit at position 10
// Special: remainder 10 is invalid
// ======================================================================
CheckResult CheckMethods::Method_31(const std::string& account, const std::string& blz) {
    int sum = 0;

    for (int i = 0; i < 9; i++) {
        sum += (account[i] - '0') * WEIGHTS_31[i];
    }

    int check_digit = sum % 11;

    if (check_digit == 10) {
        return CheckResult::INVALID_KTO;
    }

    int expected = account[9] - '0';
    return (check_digit == expected) ? CheckResult::OK : CheckResult::FALSE;
}

// ======================================================================
// Method 32: Modulus 11, Gewichtung 2,3,4,5,6,7
// Positions 4-9, check digit at position 10
// Modified like method 06
// ======================================================================
CheckResult CheckMethods::Method_32(const std::string& account, const std::string& blz) {
    int sum = 0;

    for (int i = 0; i < 6; i++) {
        sum += (account[i + 3] - '0') * WEIGHTS_32[i];  // Positions 4-9 (index 3-8)
    }

    int remainder = sum % 11;
    int check_digit;

    if (remainder <= 1) {
        check_digit = 0;
    } else {
        check_digit = 11 - remainder;
    }

    int expected = account[9] - '0';
    return (check_digit == expected) ? CheckResult::OK : CheckResult::FALSE;
}

// ======================================================================
// Method 33: Modulus 11, Gewichtung 2,3,4,5,6
// Positions 5-9, check digit at position 10
// Modified like method 06
// ======================================================================
CheckResult CheckMethods::Method_33(const std::string& account, const std::string& blz) {
    int sum = 0;

    for (int i = 0; i < 5; i++) {
        sum += (account[i + 4] - '0') * WEIGHTS_33[i];  // Positions 5-9 (index 4-8)
    }

    int remainder = sum % 11;
    int check_digit;

    if (remainder <= 1) {
        check_digit = 0;
    } else {
        check_digit = 11 - remainder;
    }

    int expected = account[9] - '0';
    return (check_digit == expected) ? CheckResult::OK : CheckResult::FALSE;
}

// ======================================================================
// Method 34: Modulus 11, Gewichtung 2,4,8,5,A,9,7 (A=10)
// Positions 1-7, check digit at position 8
// Modified like method 28
// ======================================================================
CheckResult CheckMethods::Method_34(const std::string& account, const std::string& blz) {
    int sum = 0;

    for (int i = 0; i < 7; i++) {
        sum += (account[i] - '0') * WEIGHTS_34[i];
    }

    int remainder = sum % 11;
    int check_digit;

    if (remainder <= 1) {
        check_digit = 0;
    } else {
        check_digit = 11 - remainder;
    }

    int expected = account[7] - '0';  // Check digit at position 8 (index 7)
    return (check_digit == expected) ? CheckResult::OK : CheckResult::FALSE;
}

// ======================================================================
// Method 35: Modulus 11, Gewichtung 2,3,4,5,6,7,8,9,10
// Special: remainder 10 is valid if digits at positions 9 and 10 are identical
// ======================================================================
CheckResult CheckMethods::Method_35(const std::string& account, const std::string& blz) {
    int sum = 0;

    for (int i = 0; i < 9; i++) {
        sum += (account[i] - '0') * WEIGHTS_35[i];
    }

    int check_digit = sum % 11;

    if (check_digit == 10) {
        // Special case: if digits at position 9 and 10 are identical, it's valid
        if (account[8] == account[9]) {
            return CheckResult::OK;
        } else {
            return CheckResult::INVALID_KTO;
        }
    }

    int expected = account[9] - '0';
    return (check_digit == expected) ? CheckResult::OK : CheckResult::FALSE;
}

// ======================================================================
// Method 36: Modulus 11, Gewichtung 2,4,8,5
// Positions 6-9, check digit at position 10
// Modified like method 06
// ======================================================================
CheckResult CheckMethods::Method_36(const std::string& account, const std::string& blz) {
    int sum = 0;

    for (int i = 0; i < 4; i++) {
        sum += (account[i + 5] - '0') * WEIGHTS_36[i];  // Positions 6-9 (index 5-8)
    }

    int remainder = sum % 11;
    int check_digit;

    if (remainder <= 1) {
        check_digit = 0;
    } else {
        check_digit = 11 - remainder;
    }

    int expected = account[9] - '0';
    return (check_digit == expected) ? CheckResult::OK : CheckResult::FALSE;
}

// ======================================================================
// Method 37: Modulus 11, Gewichtung 2,4,8,5,A (A=10)
// Positions 5-9, check digit at position 10
// Modified like method 06
// ======================================================================
CheckResult CheckMethods::Method_37(const std::string& account, const std::string& blz) {
    int sum = 0;

    for (int i = 0; i < 5; i++) {
        sum += (account[i + 4] - '0') * WEIGHTS_37[i];  // Positions 5-9 (index 4-8)
    }

    int remainder = sum % 11;
    int check_digit;

    if (remainder <= 1) {
        check_digit = 0;
    } else {
        check_digit = 11 - remainder;
    }

    int expected = account[9] - '0';
    return (check_digit == expected) ? CheckResult::OK : CheckResult::FALSE;
}

// ======================================================================
// Method 38: Modulus 11, Gewichtung 2,4,8,5,A,9 (A=10)
// Positions 4-9, check digit at position 10
// Modified like method 06
// ======================================================================
CheckResult CheckMethods::Method_38(const std::string& account, const std::string& blz) {
    int sum = 0;

    for (int i = 0; i < 6; i++) {
        sum += (account[i + 3] - '0') * WEIGHTS_38[i];  // Positions 4-9 (index 3-8)
    }

    int remainder = sum % 11;
    int check_digit;

    if (remainder <= 1) {
        check_digit = 0;
    } else {
        check_digit = 11 - remainder;
    }

    int expected = account[9] - '0';
    return (check_digit == expected) ? CheckResult::OK : CheckResult::FALSE;
}

// ======================================================================
// Method 39: Modulus 11, Gewichtung 2,4,8,5,A,9,7 (A=10)
// Positions 3-9, check digit at position 10
// Modified like method 06
// ======================================================================
CheckResult CheckMethods::Method_39(const std::string& account, const std::string& blz) {
    int sum = 0;

    for (int i = 0; i < 7; i++) {
        sum += (account[i + 2] - '0') * WEIGHTS_39[i];  // Positions 3-9 (index 2-8)
    }

    int remainder = sum % 11;
    int check_digit;

    if (remainder <= 1) {
        check_digit = 0;
    } else {
        check_digit = 11 - remainder;
    }

    int expected = account[9] - '0';
    return (check_digit == expected) ? CheckResult::OK : CheckResult::FALSE;
}

// ======================================================================
// Method 40: Modulus 11, Gewichtung 2,4,8,5,A,9,7,3,6 (A=10)
// All positions 1-9, check digit at position 10
// Modified like method 06
// ======================================================================
CheckResult CheckMethods::Method_40(const std::string& account, const std::string& blz) {
    int sum = 0;

    for (int i = 0; i < 9; i++) {
        sum += (account[i] - '0') * WEIGHTS_40[i];
    }

    int remainder = sum % 11;
    int check_digit;

    if (remainder <= 1) {
        check_digit = 0;
    } else {
        check_digit = 11 - remainder;
    }

    int expected = account[9] - '0';
    return (check_digit == expected) ? CheckResult::OK : CheckResult::FALSE;
}

// ======================================================================
// Method 41: Modulus 10, Gewichtung 2,1,2,1,2,1,2,1,2 (modified)
// Special: If position 4 is '9', only check positions 4-9
// Otherwise check all positions 1-9
// ======================================================================
CheckResult CheckMethods::Method_41(const std::string& account, const std::string& blz) {
    int sum = 0;

    if (account[3] == '9') {
        // Only positions 4-9 (index 3-8)
        for (int i = 3; i < 9; i++) {
            int digit = account[i] - '0';
            int weight = ((i - 3) % 2 == 0) ? 1 : 2;
            int weighted = digit * weight;

            // Cross-sum for products >= 10
            if (weighted >= 10) {
                sum += (weighted / 10) + (weighted % 10);
            } else {
                sum += weighted;
            }
        }
    } else {
        // All positions 1-9 (Method 00 logic)
        for (int i = 0; i < 9; i++) {
            int digit = account[i] - '0';
            int weight = (i % 2 == 0) ? 2 : 1;
            int weighted = digit * weight;

            // Cross-sum for products >= 10
            if (weighted >= 10) {
                sum += (weighted / 10) + (weighted % 10);
            } else {
                sum += weighted;
            }
        }
    }

    int check_digit = (10 - (sum % 10)) % 10;
    int expected = account[9] - '0';

    return (check_digit == expected) ? CheckResult::OK : CheckResult::FALSE;
}

// ======================================================================
// Method 42: Modulus 11, Gewichtung 2,3,4,5,6,7,8,9
// Positions 2-9, check digit at position 10
// Modified like method 06
// ======================================================================
CheckResult CheckMethods::Method_42(const std::string& account, const std::string& blz) {
    int sum = 0;

    for (int i = 0; i < 8; i++) {
        sum += (account[i + 1] - '0') * WEIGHTS_42[i];  // Positions 2-9 (index 1-8)
    }

    int remainder = sum % 11;
    int check_digit;

    if (remainder <= 1) {
        check_digit = 0;
    } else {
        check_digit = 11 - remainder;
    }

    int expected = account[9] - '0';
    return (check_digit == expected) ? CheckResult::OK : CheckResult::FALSE;
}

// ======================================================================
// Method 43: Modulus 10, Gewichtung 1,2,3,4,5,6,7,8,9
// Positions 1-9, check digit at position 10
// Result is (10 - (sum % 10)) % 10
// ======================================================================
CheckResult CheckMethods::Method_43(const std::string& account, const std::string& blz) {
    int sum = 0;

    for (int i = 0; i < 9; i++) {
        sum += (account[i] - '0') * WEIGHTS_43[i];
    }

    int check_digit = (10 - (sum % 10)) % 10;
    int expected = account[9] - '0';

    return (check_digit == expected) ? CheckResult::OK : CheckResult::FALSE;
}

// ======================================================================
// Method 44: Modulus 11, Gewichtung 2,4,8,5,A (A=10)
// Same as Method 37 (positions 5-9)
// ======================================================================
CheckResult CheckMethods::Method_44(const std::string& account, const std::string& blz) {
    return Method_37(account, blz);
}

// ======================================================================
// Method 45: Modulus 10, Gewichtung 2,1,2,1,2,1,2,1,2
// Like Method 00, but skip check if position 1 is '0' or position 5 is '1'
// ======================================================================
CheckResult CheckMethods::Method_45(const std::string& account, const std::string& blz) {
    // Exception: no check if position 1 is '0' or position 5 is '1'
    if (account[0] == '0' || account[4] == '1') {
        return CheckResult::OK;  // No check digit validation
    }

    // Otherwise use Method 00
    return Method_00(account, blz);
}

// ======================================================================
// Method 46: Modulus 11, Gewichtung 2,3,4,5,6
// Positions 3-7, check digit at position 8
// Modified like method 06
// ======================================================================
CheckResult CheckMethods::Method_46(const std::string& account, const std::string& blz) {
    static const int weights[] = {6, 5, 4, 3, 2};
    int sum = 0;

    for (int i = 0; i < 5; i++) {
        sum += (account[i + 2] - '0') * weights[i];  // Positions 3-7 (index 2-6)
    }

    int remainder = sum % 11;
    int check_digit;

    if (remainder <= 1) {
        check_digit = 0;
    } else {
        check_digit = 11 - remainder;
    }

    int expected = account[7] - '0';  // Check digit at position 8 (index 7)
    return (check_digit == expected) ? CheckResult::OK : CheckResult::FALSE;
}

// ======================================================================
// Method 47: Modulus 11, Gewichtung 2,3,4,5,6
// Positions 4-8, check digit at position 9
// Modified like method 06
// ======================================================================
CheckResult CheckMethods::Method_47(const std::string& account, const std::string& blz) {
    static const int weights[] = {6, 5, 4, 3, 2};
    int sum = 0;

    for (int i = 0; i < 5; i++) {
        sum += (account[i + 3] - '0') * weights[i];  // Positions 4-8 (index 3-7)
    }

    int remainder = sum % 11;
    int check_digit;

    if (remainder <= 1) {
        check_digit = 0;
    } else {
        check_digit = 11 - remainder;
    }

    int expected = account[8] - '0';  // Check digit at position 9 (index 8)
    return (check_digit == expected) ? CheckResult::OK : CheckResult::FALSE;
}

// ======================================================================
// Method 48: Modulus 11, Gewichtung 2,3,4,5,6,7
// Positions 3-8, check digit at position 9
// Modified like method 06
// ======================================================================
CheckResult CheckMethods::Method_48(const std::string& account, const std::string& blz) {
    static const int weights[] = {7, 6, 5, 4, 3, 2};
    int sum = 0;

    for (int i = 0; i < 6; i++) {
        sum += (account[i + 2] - '0') * weights[i];  // Positions 3-8 (index 2-7)
    }

    int remainder = sum % 11;
    int check_digit;

    if (remainder <= 1) {
        check_digit = 0;
    } else {
        check_digit = 11 - remainder;
    }

    int expected = account[8] - '0';  // Check digit at position 9 (index 8)
    return (check_digit == expected) ? CheckResult::OK : CheckResult::FALSE;
}

// ======================================================================
// Method 49: Two variants - try Method 00, if fails try Method 01
// ======================================================================
CheckResult CheckMethods::Method_49(const std::string& account, const std::string& blz) {
    // Variant 1: Try Method 00
    CheckResult result = Method_00(account, blz);
    if (result == CheckResult::OK) {
        return CheckResult::OK;
    }

    // Variant 2: Try Method 01
    return Method_01(account, blz);
}

// ======================================================================
// Method 50: Two-variant MOD-11, positions 1-6, check digit at position 7
// If variant 1 fails, shift account left by 3 positions
// ======================================================================
CheckResult CheckMethods::Method_50(const std::string& account, const std::string& blz) {
    // Variant 1: Check digit at position 7
    int sum = 0;
    for (int i = 0; i < 6; i++) {
        sum += (account[i] - '0') * WEIGHTS_50[i];  // Positions 1-6 (index 0-5)
    }

    int remainder = sum % 11;
    int check_digit;

    if (remainder <= 1) {
        check_digit = 0;
    } else {
        check_digit = 11 - remainder;
    }

    int expected = account[6] - '0';  // Check digit at position 7 (index 6)
    if (check_digit == expected) {
        return CheckResult::OK;
    }

    // Variant 2: Shift account left by 3, check digit at position 10
    // Account positions 4-9 with same weights, check digit at position 10
    sum = 0;
    for (int i = 0; i < 6; i++) {
        sum += (account[i + 3] - '0') * WEIGHTS_50[i];  // Positions 4-9 (index 3-8)
    }

    remainder = sum % 11;

    if (remainder <= 1) {
        check_digit = 0;
    } else {
        check_digit = 11 - remainder;
    }

    expected = account[9] - '0';  // Check digit at position 10 (index 9)
    return (check_digit == expected) ? CheckResult::OK : CheckResult::FALSE;
}

// ======================================================================
// Method 51: Complex multi-variant method with special case for position 3='9'
// ======================================================================
CheckResult CheckMethods::Method_51(const std::string& account, const std::string& blz) {
    // Check if position 3 is '9' (Sachkonten - special case)
    if (account[2] == '9') {
        // Exception Variant 1: Positions 3-9, weights 8,7,6,5,4,3,2
        int sum = 9 * 8;  // Position 3 is '9'
        for (int i = 0; i < 6; i++) {
            sum += (account[i + 3] - '0') * WEIGHTS_51_EX1[i + 1];  // Positions 4-9
        }

        int remainder = sum % 11;
        int check_digit = (remainder <= 1) ? 0 : (11 - remainder);

        if (check_digit == (account[9] - '0')) {
            return CheckResult::OK;
        }

        // Exception Variant 2: All positions, weights 10,9,8,7,6,5,4,3,2
        sum = 0;
        sum += (account[0] - '0') * 10;
        sum += (account[1] - '0') * 9;
        sum += 9 * 8;  // Position 3 is '9'
        for (int i = 3; i < 9; i++) {
            sum += (account[i] - '0') * WEIGHTS_51_EX2[i];
        }

        remainder = sum % 11;
        check_digit = (remainder <= 1) ? 0 : (11 - remainder);

        if (check_digit == (account[9] - '0')) {
            return CheckResult::OK;
        }

        return CheckResult::FALSE;
    }

    // Method A: Positions 4-9, weights 7,6,5,4,3,2
    int sum = 0;
    for (int i = 0; i < 6; i++) {
        sum += (account[i + 3] - '0') * WEIGHTS_51_A[i];  // Positions 4-9
    }

    int remainder = sum % 11;
    int check_digit = (remainder <= 1) ? 0 : (11 - remainder);

    if (check_digit == (account[9] - '0')) {
        return CheckResult::OK;
    }

    // Method B: Positions 5-9, weights 6,5,4,3,2
    sum = 0;
    for (int i = 0; i < 5; i++) {
        sum += (account[i + 4] - '0') * WEIGHTS_51_B[i];  // Positions 5-9
    }

    remainder = sum % 11;
    check_digit = (remainder <= 1) ? 0 : (11 - remainder);

    if (check_digit == (account[9] - '0')) {
        return CheckResult::OK;
    }

    // Method C: Positions 5-9, weights 6,5,4,3,2, but MOD 7
    // Check if position 10 is 7, 8, or 9 (invalid for Method C)
    if (account[9] == '7' || account[9] == '8' || account[9] == '9') {
        return CheckResult::FALSE;
    }

    sum = 0;
    for (int i = 0; i < 5; i++) {
        sum += (account[i + 4] - '0') * WEIGHTS_51_B[i];  // Positions 5-9
    }

    remainder = sum % 7;
    check_digit = (remainder == 0) ? 0 : (7 - remainder);

    return (check_digit == (account[9] - '0')) ? CheckResult::OK : CheckResult::FALSE;
}

// ======================================================================
// Method 52: Complex ESER-Altsystem transformation (requires BLZ)
// For now, return NOT_IMPLEMENTED as this requires complex BLZ-based transformation
// ======================================================================
CheckResult CheckMethods::Method_52(const std::string& account, const std::string& blz) {
    // If account starts with '9', use Method 20
    if (account[0] == '9') {
        return Method_20(account, blz);
    }

    // Full implementation requires ESER-Altsystem transformation with BLZ
    // This is complex and requires additional infrastructure
    return CheckResult::NOT_IMPLEMENTED;
}

// ======================================================================
// Method 53: Similar to Method 52 but for 9-digit accounts
// For now, return NOT_IMPLEMENTED as this requires complex BLZ-based transformation
// ======================================================================
CheckResult CheckMethods::Method_53(const std::string& account, const std::string& blz) {
    // If account starts with '9', use Method 20
    if (account[0] == '9') {
        return Method_20(account, blz);
    }

    // Account must be 9-digit (first digit '0', second digit not '0')
    if (account[0] != '0' || account[1] == '0') {
        return CheckResult::INVALID_KTO;
    }

    // Full implementation requires ESER-Altsystem transformation with BLZ
    return CheckResult::NOT_IMPLEMENTED;
}

// ======================================================================
// Method 54: MOD-11 with fixed prefix "49"
// ======================================================================
CheckResult CheckMethods::Method_54(const std::string& account, const std::string& blz) {
    // Account must start with "49"
    if (account[0] != '4' || account[1] != '9') {
        return CheckResult::INVALID_KTO;
    }

    int sum = 0;
    for (int i = 0; i < 7; i++) {
        sum += (account[i + 2] - '0') * WEIGHTS_54[i];  // Positions 3-9 (index 2-8)
    }

    int remainder = sum % 11;
    int check_digit = 11 - remainder;

    // Check digit must be single digit (reject if remainder 0 or 1 which give 11 or 10)
    if (check_digit > 9) {
        return CheckResult::INVALID_KTO;
    }

    int expected = account[9] - '0';  // Check digit at position 10 (index 9)
    return (check_digit == expected) ? CheckResult::OK : CheckResult::FALSE;
}

// ======================================================================
// Method 55: MOD-11 with weights 8,7,8,7,6,5,4,3,2
// ======================================================================
CheckResult CheckMethods::Method_55(const std::string& account, const std::string& blz) {
    int sum = 0;
    for (int i = 0; i < 9; i++) {
        sum += (account[i] - '0') * WEIGHTS_55[i];
    }

    int remainder = sum % 11;
    int check_digit;

    if (remainder <= 1) {
        check_digit = 0;
    } else {
        check_digit = 11 - remainder;
    }

    int expected = account[9] - '0';  // Check digit at position 10 (index 9)
    return (check_digit == expected) ? CheckResult::OK : CheckResult::FALSE;
}

// ======================================================================
// Method 56: MOD-11 with special handling for accounts starting with '9'
// ======================================================================
CheckResult CheckMethods::Method_56(const std::string& account, const std::string& blz) {
    int sum = 0;
    for (int i = 0; i < 9; i++) {
        sum += (account[i] - '0') * WEIGHTS_56[i];
    }

    int remainder = sum % 11;
    int check_digit = 11 - remainder;

    // Special handling if result is 10 or 11
    if (check_digit > 9) {
        if (account[0] == '9') {
            // For accounts starting with '9': 10->7, 11->8
            check_digit = (check_digit == 10) ? 7 : 8;
        } else {
            return CheckResult::INVALID_KTO;
        }
    }

    int expected = account[9] - '0';  // Check digit at position 10 (index 9)
    return (check_digit == expected) ? CheckResult::OK : CheckResult::FALSE;
}

// ======================================================================
// Method 57: MOD-10 with many exceptions (skip validation for certain patterns)
// ======================================================================
CheckResult CheckMethods::Method_57(const std::string& account, const std::string& blz) {
    // Check exceptions - no validation for these patterns
    int first_two = (account[0] - '0') * 10 + (account[1] - '0');

    // Skip validation for: 00-50, 91, 96-99
    if (first_two <= 50 || first_two == 91 || first_two >= 96) {
        return CheckResult::OK;
    }

    // Skip validation for accounts starting with "777777" or "888888"
    if (std::strncmp(account.c_str(), "777777", 6) == 0 ||
        std::strncmp(account.c_str(), "888888", 6) == 0) {
        return CheckResult::OK;
    }

    // Otherwise, use Method 00
    return Method_00(account, blz);
}

// ======================================================================
// Method 58: MOD-11 on positions 5-9, weights 6,5,4,3,2
// ======================================================================
CheckResult CheckMethods::Method_58(const std::string& account, const std::string& blz) {
    int sum = 0;
    for (int i = 0; i < 5; i++) {
        sum += (account[i + 4] - '0') * WEIGHTS_58[i];  // Positions 5-9 (index 4-8)
    }

    int remainder = sum % 11;
    int check_digit = (remainder == 0) ? 0 : (11 - remainder);

    // Reject if result is 10
    if (check_digit == 10) {
        return CheckResult::INVALID_KTO;
    }

    int expected = account[9] - '0';  // Check digit at position 10 (index 9)
    return (check_digit == expected) ? CheckResult::OK : CheckResult::FALSE;
}

// ======================================================================
// Method 59: MOD-10 like Method 00, but skip accounts < 9 digits
// ======================================================================
CheckResult CheckMethods::Method_59(const std::string& account, const std::string& blz) {
    // Skip validation if first two digits are "00" (account < 9 digits)
    if (account[0] == '0' && account[1] == '0') {
        return CheckResult::OK;
    }

    // Otherwise, use Method 00
    return Method_00(account, blz);
}

// ======================================================================
// Method 60: MOD-10, skip positions 1-2 (sub-account), positions 3-9
// ======================================================================
CheckResult CheckMethods::Method_60(const std::string& account, const std::string& blz) {
    // Skip first two positions (sub-account), validate positions 3-9
    // Use MOD-10 with cross-sum like Method 00
    int sum = 0;

    for (int i = 2; i < 9; i++) {
        int digit = account[i] - '0';
        if ((i - 2) % 2 == 0) {  // Even positions in our range (2,4,6,8)
            int product = digit * 2;
            sum += (product >= 10) ? (product - 9) : product;
        } else {
            sum += digit;
        }
    }

    int remainder = sum % 10;
    int check_digit = (remainder == 0) ? 0 : (10 - remainder);

    int expected = account[9] - '0';  // Check digit at position 10 (index 9)
    return (check_digit == expected) ? CheckResult::OK : CheckResult::FALSE;
}

// ======================================================================
// Method 61: MOD-10, two variants based on position 9
// ======================================================================
CheckResult CheckMethods::Method_61(const std::string& account, const std::string& blz) {
    // If position 9 (index 8) is '8', use variant 2 (include positions 9-10)
    // Otherwise, use variant 1 (positions 1-7 only)

    if (account[8] == '8') {
        // Variant 2: Include positions 1-7 and 9-10 in validation
        int sum = 0;

        for (int i = 0; i < 7; i++) {
            int digit = account[i] - '0';
            if (i % 2 == 0) {  // Positions 1,3,5,7
                int product = digit * 2;
                sum += (product >= 10) ? (product - 9) : product;
            } else {
                sum += digit;
            }
        }

        // Add position 9 (index 8)
        sum += (account[8] - '0');

        // Add position 10 (index 9) with weight 2
        int digit = account[9] - '0';
        int product = digit * 2;
        sum += (product >= 10) ? (product - 9) : product;

        int remainder = sum % 10;
        int check_digit = (remainder == 0) ? 0 : (10 - remainder);

        // Check digit is at position 8 (index 7)
        int expected = account[7] - '0';
        return (check_digit == expected) ? CheckResult::OK : CheckResult::FALSE;
    } else {
        // Variant 1: Positions 1-7 only
        int sum = 0;

        for (int i = 0; i < 7; i++) {
            int digit = account[i] - '0';
            if (i % 2 == 0) {  // Positions 1,3,5,7
                int product = digit * 2;
                sum += (product >= 10) ? (product - 9) : product;
            } else {
                sum += digit;
            }
        }

        int remainder = sum % 10;
        int check_digit = (remainder == 0) ? 0 : (10 - remainder);

        // Check digit is at position 8 (index 7)
        int expected = account[7] - '0';
        return (check_digit == expected) ? CheckResult::OK : CheckResult::FALSE;
    }
}

// ======================================================================
// Method 62: MOD-10, positions 3-7, check digit at position 8
// ======================================================================
CheckResult CheckMethods::Method_62(const std::string& account, const std::string& blz) {
    // Skip positions 1-2 and 9-10, validate positions 3-7
    int sum = 0;

    for (int i = 2; i < 7; i++) {
        int digit = account[i] - '0';
        if ((i - 2) % 2 == 0) {  // Even positions in our range (2,4,6)
            int product = digit * 2;
            sum += (product >= 10) ? (product - 9) : product;
        } else {
            sum += digit;
        }
    }

    int remainder = sum % 10;
    int check_digit = (remainder == 0) ? 0 : (10 - remainder);

    int expected = account[7] - '0';  // Check digit at position 8 (index 7)
    return (check_digit == expected) ? CheckResult::OK : CheckResult::FALSE;
}

// ======================================================================
// Method 63: MOD-10, complex with two variants based on leading zeros
// ======================================================================
CheckResult CheckMethods::Method_63(const std::string& account, const std::string& blz) {
    // First position must be '0'
    if (account[0] != '0') {
        return CheckResult::INVALID_KTO;
    }

    // Check if positions 2-3 are "00" (alternative format)
    if (account[1] == '0' && account[2] == '0') {
        // Alternative format: positions 4-9, check digit at position 10
        int sum = 0;

        for (int i = 3; i < 9; i++) {
            int digit = account[i] - '0';
            if ((i - 3) % 2 == 1) {  // Odd positions in our range (4,6,8)
                int product = digit * 2;
                sum += (product >= 10) ? (product - 9) : product;
            } else {
                sum += digit;
            }
        }

        int remainder = sum % 10;
        int check_digit = (remainder == 0) ? 0 : (10 - remainder);

        int expected = account[9] - '0';  // Check digit at position 10 (index 9)
        return (check_digit == expected) ? CheckResult::OK : CheckResult::FALSE;
    } else {
        // Standard format: positions 2-7, check digit at position 8
        int sum = 0;

        for (int i = 1; i < 7; i++) {
            int digit = account[i] - '0';
            if ((i - 1) % 2 == 1) {  // Odd positions in our range (2,4,6)
                int product = digit * 2;
                sum += (product >= 10) ? (product - 9) : product;
            } else {
                sum += digit;
            }
        }

        int remainder = sum % 10;
        int check_digit = (remainder == 0) ? 0 : (10 - remainder);

        int expected = account[7] - '0';  // Check digit at position 8 (index 7)
        return (check_digit == expected) ? CheckResult::OK : CheckResult::FALSE;
    }
}

// ======================================================================
// Method 64: MOD-11 with weights 9,10,5,8,4,2, positions 1-6
// ======================================================================
CheckResult CheckMethods::Method_64(const std::string& account, const std::string& blz) {
    int sum = 0;
    for (int i = 0; i < 6; i++) {
        sum += (account[i] - '0') * WEIGHTS_64[i];
    }

    int remainder = sum % 11;
    int check_digit;

    if (remainder <= 1) {
        check_digit = 0;
    } else {
        check_digit = 11 - remainder;
    }

    int expected = account[6] - '0';  // Check digit at position 7 (index 6)
    return (check_digit == expected) ? CheckResult::OK : CheckResult::FALSE;
}

// ======================================================================
// Method 65: MOD-10 like Method 00 with exception for position 9='9'
// ======================================================================
CheckResult CheckMethods::Method_65(const std::string& account, const std::string& blz) {
    // Standard calculation for positions 1-7
    int sum = 0;

    for (int i = 0; i < 7; i++) {
        int digit = account[i] - '0';
        if (i % 2 == 0) {  // Positions 1,3,5,7
            int product = digit * 2;
            sum += (product >= 10) ? (product - 9) : product;
        } else {
            sum += digit;
        }
    }

    // If position 9 (index 8) is '9', include positions 9-10 in calculation
    if (account[8] == '9') {
        // Add 9 (position 9)
        int product = 9 * 2;  // Weight 2 for position 9
        sum += (product >= 10) ? (product - 9) : product;

        // Add position 10 with weight 2
        int digit = account[9] - '0';
        product = digit * 2;
        sum += (product >= 10) ? (product - 9) : product;
    }

    int remainder = sum % 10;
    int check_digit = (remainder == 0) ? 0 : (10 - remainder);

    int expected = account[7] - '0';  // Check digit at position 8 (index 7)
    return (check_digit == expected) ? CheckResult::OK : CheckResult::FALSE;
}

// ======================================================================
// Method 66: MOD-11 with special handling, skip positions 3-4
// ======================================================================
CheckResult CheckMethods::Method_66(const std::string& account, const std::string& blz) {
    // First position must be '0'
    if (account[0] != '0') {
        return CheckResult::INVALID_KTO;
    }

    // Positions: 2, 5-9 (skip 3-4)
    // Weights: 7, 6, 5, 4, 3, 2
    int sum = (account[1] - '0') * 7;

    for (int i = 0; i < 5; i++) {
        sum += (account[i + 4] - '0') * WEIGHTS_66[i + 1];  // Positions 5-9
    }

    int remainder = sum % 11;
    int check_digit;

    if (remainder < 2) {
        check_digit = 1 - remainder;  // 0->1, 1->0
    } else {
        check_digit = 11 - remainder;
    }

    int expected = account[9] - '0';  // Check digit at position 10 (index 9)
    return (check_digit == expected) ? CheckResult::OK : CheckResult::FALSE;
}

// ======================================================================
// Method 67: MOD-10, positions 1-7, skip positions 9-10
// ======================================================================
CheckResult CheckMethods::Method_67(const std::string& account, const std::string& blz) {
    // Positions 1-7, check digit at position 8
    int sum = 0;

    for (int i = 0; i < 7; i++) {
        int digit = account[i] - '0';
        if (i % 2 == 0) {  // Positions 1,3,5,7
            int product = digit * 2;
            sum += (product >= 10) ? (product - 9) : product;
        } else {
            sum += digit;
        }
    }

    int remainder = sum % 10;
    int check_digit = (remainder == 0) ? 0 : (10 - remainder);

    int expected = account[7] - '0';  // Check digit at position 8 (index 7)
    return (check_digit == expected) ? CheckResult::OK : CheckResult::FALSE;
}

// ======================================================================
// Method 68: Complex multi-variant based on account length
// ======================================================================
CheckResult CheckMethods::Method_68(const std::string& account, const std::string& blz) {
    // Special case: accounts 04xxxxxxxx have no check digit
    if (account[0] == '0' && account[1] == '4') {
        return CheckResult::OK;
    }

    // 10-digit accounts (first digit not '0')
    if (account[0] != '0') {
        // Position 4 must be '9'
        if (account[3] != '9') {
            return CheckResult::INVALID_KTO;
        }

        // Validate positions 5-9 (position 7 from right is fixed as '9')
        int sum = 9;  // Position 7 from right (position 4) is '9'

        for (int i = 4; i < 9; i++) {
            int digit = account[i] - '0';
            if ((i - 4) % 2 == 0) {  // Even positions
                int product = digit * 2;
                sum += (product >= 10) ? (product - 9) : product;
            } else {
                sum += digit;
            }
        }

        int remainder = sum % 10;
        int check_digit = (remainder == 0) ? 0 : (10 - remainder);

        int expected = account[9] - '0';
        return (check_digit == expected) ? CheckResult::OK : CheckResult::FALSE;
    }

    // 6-9 digit accounts: Try variant 1 (full), then variant 2 (skip positions 3-4)

    // Variant 1: Positions 2-9
    int sum = 0;
    for (int i = 1; i < 9; i++) {
        int digit = account[i] - '0';
        if ((i - 1) % 2 == 1) {  // Odd positions in our range
            int product = digit * 2;
            sum += (product >= 10) ? (product - 9) : product;
        } else {
            sum += digit;
        }
    }

    int remainder = sum % 10;
    int check_digit = (remainder == 0) ? 0 : (10 - remainder);
    int expected = account[9] - '0';

    if (check_digit == expected) {
        return CheckResult::OK;
    }

    // Variant 2: Skip positions 3-4, use positions 2,5-9
    sum = (account[1] - '0');  // Position 2

    for (int i = 4; i < 9; i++) {
        int digit = account[i] - '0';
        if ((i - 4) % 2 == 0) {  // Even positions
            int product = digit * 2;
            sum += (product >= 10) ? (product - 9) : product;
        } else {
            sum += digit;
        }
    }

    remainder = sum % 10;
    check_digit = (remainder == 0) ? 0 : (10 - remainder);

    return (check_digit == expected) ? CheckResult::OK : CheckResult::FALSE;
}

// ======================================================================
// Method 69: Two variants - MOD-11 or M10H transformation
// ======================================================================
CheckResult CheckMethods::Method_69(const std::string& account, const std::string& blz) {
    // Special case: 93xxxxxxxx - no check digit
    if (account[0] == '9' && account[1] == '3') {
        return CheckResult::OK;
    }

    // Variant 1: MOD-11 for non-97xxxxxxxx accounts
    if (account[0] != '9' || account[1] != '7') {
        int sum = 0;
        for (int i = 0; i < 7; i++) {
            sum += (account[i] - '0') * WEIGHTS_69[i];
        }

        int remainder = sum % 11;
        int check_digit = (remainder <= 1) ? 0 : (11 - remainder);

        int expected = account[7] - '0';  // Check digit at position 8 (index 7)
        if (check_digit == expected) {
            return CheckResult::OK;
        }
    }

    // Variant 2: M10H transformation (for 97xxxxxxxx or if variant 1 failed)
    int sum = M10H_DIGITS[0][account[0] - '0']
            + M10H_DIGITS[3][account[1] - '0']
            + M10H_DIGITS[2][account[2] - '0']
            + M10H_DIGITS[1][account[3] - '0']
            + M10H_DIGITS[0][account[4] - '0']
            + M10H_DIGITS[3][account[5] - '0']
            + M10H_DIGITS[2][account[6] - '0']
            + M10H_DIGITS[1][account[7] - '0']
            + M10H_DIGITS[0][account[8] - '0'];

    int remainder = sum % 10;
    int check_digit = (remainder == 0) ? 0 : (10 - remainder);

    int expected = account[9] - '0';  // Check digit at position 10 (index 9)
    return (check_digit == expected) ? CheckResult::OK : CheckResult::FALSE;
}

// ======================================================================
// Method 70: MOD-11 with exceptions for position 4='5' or positions 4-5='69'
// ======================================================================
CheckResult CheckMethods::Method_70(const std::string& account, const std::string& blz) {
    int sum;
    int remainder;
    int check_digit;

    // Exception: position 4 = '5'
    if (account[3] == '5') {
        sum = 5 * 7;
        for (int i = 0; i < 5; i++) {
            sum += (account[i + 4] - '0') * WEIGHTS_70_V2[i];  // Positions 5-9
        }
        remainder = sum % 11;
    }
    // Exception: positions 4-5 = '69'
    else if (account[3] == '6' && account[4] == '9') {
        sum = 6 * 7 + 9 * 6;
        for (int i = 0; i < 4; i++) {
            sum += (account[i + 5] - '0') * (5 - i);  // Positions 6-9, weights 5,4,3,2
        }
        remainder = sum % 11;
    }
    // Standard: all positions 1-9
    else {
        sum = 0;
        for (int i = 0; i < 9; i++) {
            sum += (account[i] - '0') * WEIGHTS_70_V1[i];
        }
        remainder = sum % 11;
    }

    check_digit = (remainder <= 1) ? 0 : (11 - remainder);

    int expected = account[9] - '0';  // Check digit at position 10 (index 9)
    return (check_digit == expected) ? CheckResult::OK : CheckResult::FALSE;
}

// ======================================================================
// Method 71: MOD-11 positions 2-7, special handling for remainder 1
// ======================================================================
CheckResult CheckMethods::Method_71(const std::string& account, const std::string& blz) {
    int sum = 0;
    for (int i = 0; i < 6; i++) {
        sum += (account[i + 1] - '0') * WEIGHTS_71[i];  // Positions 2-7 (index 1-6)
    }

    int remainder = sum % 11;
    int check_digit;

    if (remainder > 1) {
        check_digit = 11 - remainder;
    } else {
        check_digit = remainder;  // 0->0, 1->1 (special handling)
    }

    int expected = account[9] - '0';  // Check digit at position 10 (index 9)
    return (check_digit == expected) ? CheckResult::OK : CheckResult::FALSE;
}

// ======================================================================
// Method 72: MOD-10 positions 4-9, skip positions 1-3
// ======================================================================
CheckResult CheckMethods::Method_72(const std::string& account, const std::string& blz) {
    // Skip positions 1-3 (sub-account and art), use positions 4-9
    int sum = 0;

    for (int i = 3; i < 9; i++) {
        int digit = account[i] - '0';
        if ((i - 3) % 2 == 1) {  // Odd positions in our range (4,6,8)
            int product = digit * 2;
            sum += (product >= 10) ? (product - 9) : product;
        } else {
            sum += digit;
        }
    }

    int remainder = sum % 10;
    int check_digit = (remainder == 0) ? 0 : (10 - remainder);

    int expected = account[9] - '0';  // Check digit at position 10 (index 9)
    return (check_digit == expected) ? CheckResult::OK : CheckResult::FALSE;
}

// ======================================================================
// Method 73: Multi-variant with exception for position 3='9'
// ======================================================================
CheckResult CheckMethods::Method_73(const std::string& account, const std::string& blz) {
    // Exception: position 3 = '9' (Sachkonten) - same as Method 51 exception
    if (account[2] == '9') {
        // Exception Variant 1
        int sum = 9 * 8;
        for (int i = 3; i < 9; i++) {
            sum += (account[i] - '0') * (10 - i);  // Weights 7,6,5,4,3,2
        }

        int remainder = sum % 11;
        int check_digit = (remainder <= 1) ? 0 : (11 - remainder);

        if (check_digit == (account[9] - '0')) {
            return CheckResult::OK;
        }

        // Exception Variant 2
        sum = (account[0] - '0') * 10 + (account[1] - '0') * 9 + 9 * 8;
        for (int i = 3; i < 9; i++) {
            sum += (account[i] - '0') * (10 - i);
        }

        remainder = sum % 11;
        check_digit = (remainder <= 1) ? 0 : (11 - remainder);

        return (check_digit == (account[9] - '0')) ? CheckResult::OK : CheckResult::FALSE;
    }

    // Variant 1: Positions 4-9 (include position 4)
    int sum = (account[3] - '0');
    for (int i = 4; i < 9; i++) {
        int digit = account[i] - '0';
        if ((i - 4) % 2 == 0) {  // Even positions in our range
            int product = digit * 2;
            sum += (product >= 10) ? (product - 9) : product;
        } else {
            sum += digit;
        }
    }

    int remainder = sum % 10;
    int check_digit = (remainder == 0) ? 0 : (10 - remainder);

    if (check_digit == (account[9] - '0')) {
        return CheckResult::OK;
    }

    // Variant 2: Positions 5-9 (skip position 4)
    sum = 0;
    for (int i = 4; i < 9; i++) {
        int digit = account[i] - '0';
        if ((i - 4) % 2 == 0) {  // Even positions
            int product = digit * 2;
            sum += (product >= 10) ? (product - 9) : product;
        } else {
            sum += digit;
        }
    }

    remainder = sum % 10;
    check_digit = (remainder == 0) ? 0 : (10 - remainder);

    if (check_digit == (account[9] - '0')) {
        return CheckResult::OK;
    }

    // Variant 3: Positions 5-9, MOD-7
    remainder = sum % 7;
    check_digit = (remainder == 0) ? 0 : (7 - remainder);

    return (check_digit == (account[9] - '0')) ? CheckResult::OK : CheckResult::FALSE;
}

// ======================================================================
// Method 74: MOD-10 with special handling for 6-digit accounts
// ======================================================================
CheckResult CheckMethods::Method_74(const std::string& account, const std::string& blz) {
    // Standard MOD-10 calculation
    int sum = 0;

    for (int i = 0; i < 9; i++) {
        int digit = account[i] - '0';
        if (i % 2 == 0) {  // Even positions
            int product = digit * 2;
            sum += (product >= 10) ? (product - 9) : product;
        } else {
            sum += digit;
        }
    }

    int remainder = sum % 10;
    int check_digit = (remainder == 0) ? 0 : (10 - remainder);

    int expected = account[9] - '0';

    if (check_digit == expected) {
        return CheckResult::OK;
    }

    // Special case for 6-digit accounts (starts with "0000")
    if (account[0] == '0' && account[1] == '0' && account[2] == '0' && account[3] == '0') {
        // Round up to next half-decade
        check_digit = 5 - (sum % 5);
        if (check_digit == 5) {
            check_digit = 0;
        }
        return (check_digit == expected) ? CheckResult::OK : CheckResult::FALSE;
    }

    return CheckResult::FALSE;
}

// ======================================================================
// Method 75: Multiple variants based on account length
// ======================================================================
CheckResult CheckMethods::Method_75(const std::string& account, const std::string& blz) {
    // Must start with '0'
    if (account[0] != '0') {
        return CheckResult::INVALID_KTO;
    }

    // 6/7-digit accounts (starts with "00" but not "000x" where x!='0')
    if (account[0] == '0' && account[1] == '0') {
        if (account[2] != '0' || (account[2] == '0' && account[3] == '0' && account[4] == '0')) {
            return CheckResult::INVALID_KTO;  // 8-digit or <6-digit
        }

        // Positions 5-9, check digit at position 10
        int sum = 0;
        for (int i = 4; i < 9; i++) {
            int digit = account[i] - '0';
            if ((i - 4) % 2 == 0) {  // Even positions
                int product = digit * 2;
                sum += (product >= 10) ? (product - 9) : product;
            } else {
                sum += digit;
            }
        }

        int remainder = sum % 10;
        int check_digit = (remainder == 0) ? 0 : (10 - remainder);

        int expected = account[9] - '0';
        return (check_digit == expected) ? CheckResult::OK : CheckResult::FALSE;
    }

    // 9-digit account starting with "09" (variant 2)
    if (account[1] == '9') {
        // Positions 3-7, check digit at position 8
        int sum = 0;
        for (int i = 2; i < 7; i++) {
            int digit = account[i] - '0';
            if ((i - 2) % 2 == 0) {  // Even positions
                int product = digit * 2;
                sum += (product >= 10) ? (product - 9) : product;
            } else {
                sum += digit;
            }
        }

        int remainder = sum % 10;
        int check_digit = (remainder == 0) ? 0 : (10 - remainder);

        int expected = account[7] - '0';
        return (check_digit == expected) ? CheckResult::OK : CheckResult::FALSE;
    }

    // 9-digit account (variant 1)
    // Positions 2-6, check digit at position 7
    int sum = 0;
    for (int i = 1; i < 6; i++) {
        int digit = account[i] - '0';
        if ((i - 1) % 2 == 0) {  // Even positions
            int product = digit * 2;
            sum += (product >= 10) ? (product - 9) : product;
        } else {
            sum += digit;
        }
    }

    int remainder = sum % 10;
    int check_digit = (remainder == 0) ? 0 : (10 - remainder);

    int expected = account[6] - '0';
    return (check_digit == expected) ? CheckResult::OK : CheckResult::FALSE;
}

// ======================================================================
// Method 76: MOD-11 with account type validation
// ======================================================================
CheckResult CheckMethods::Method_76(const std::string& account, const std::string& blz) {
    // Check account type (position 1): must be 0, 4, 6, 7, 8, or 9
    char first = account[0];
    if (first == '1' || first == '2' || first == '3' || first == '5') {
        return CheckResult::INVALID_KTO;
    }

    // Variant 1: Positions 2-7, check digit at position 8
    int sum = 0;
    for (int i = 0; i < 6; i++) {
        sum += (account[i + 1] - '0') * WEIGHTS_76[i];
    }

    int remainder = sum % 11;

    if (remainder == 10) {
        // Try variant 2
        // Check position 3 type
        char third = account[2];
        if (third == '1' || third == '2' || third == '3' || third == '5') {
            return CheckResult::INVALID_KTO;
        }

        // Positions 4-9, check digit at position 10
        sum = 0;
        for (int i = 0; i < 6; i++) {
            sum += (account[i + 3] - '0') * WEIGHTS_76[i];
        }

        remainder = sum % 11;

        if (remainder == 10) {
            return CheckResult::INVALID_KTO;
        }

        int expected = account[9] - '0';
        return (remainder == expected) ? CheckResult::OK : CheckResult::FALSE;
    }

    int expected = account[7] - '0';
    return (remainder == expected) ? CheckResult::OK : CheckResult::FALSE;
}

// ======================================================================
// Method 77: Two-variant MOD-11 with different weights
// ======================================================================
CheckResult CheckMethods::Method_77(const std::string& account, const std::string& blz) {
    // Variant 1: Positions 6-10, weights 5,4,3,2,1
    int sum = 0;
    for (int i = 0; i < 5; i++) {
        sum += (account[i + 5] - '0') * WEIGHTS_77_V1[i];
    }

    int remainder = sum % 11;

    if (remainder == 0) {
        return CheckResult::OK;
    }

    // Variant 2: Positions 6-10, weights 5,4,3,4,5
    sum = 0;
    for (int i = 0; i < 5; i++) {
        sum += (account[i + 5] - '0') * WEIGHTS_77_V2[i];
    }

    remainder = sum % 11;

    return (remainder == 0) ? CheckResult::OK : CheckResult::FALSE;
}

// ======================================================================
// Method 78: MOD-10 like Method 00, skip 8-digit accounts
// ======================================================================
CheckResult CheckMethods::Method_78(const std::string& account, const std::string& blz) {
    // 8-digit accounts have no check digit (starts with "00" but not "000")
    if (account[0] == '0' && account[1] == '0' && account[2] != '0') {
        return CheckResult::OK;
    }

    // Otherwise, use Method 00
    return Method_00(account, blz);
}

// ======================================================================
// Method 79: Two variants based on first digit
// ======================================================================
CheckResult CheckMethods::Method_79(const std::string& account, const std::string& blz) {
    // First digit must not be '0'
    if (account[0] == '0') {
        return CheckResult::INVALID_KTO;
    }

    // Variant 2: First digit is 1, 2, or 9 - check digit at position 9
    if (account[0] == '1' || account[0] == '2' || account[0] == '9') {
        int sum = (account[0] - '0');  // Position 1 with weight 1

        for (int i = 1; i < 8; i++) {
            int digit = account[i] - '0';
            if (i % 2 == 1) {  // Odd positions (2,4,6,8)
                int product = digit * 2;
                sum += (product >= 10) ? (product - 9) : product;
            } else {
                sum += digit;
            }
        }

        int remainder = sum % 10;
        int check_digit = (remainder == 0) ? 0 : (10 - remainder);

        int expected = account[8] - '0';  // Check digit at position 9 (index 8)
        return (check_digit == expected) ? CheckResult::OK : CheckResult::FALSE;
    }

    // Variant 1: First digit is 3-8 - check digit at position 10
    int sum = 0;

    for (int i = 0; i < 9; i++) {
        int digit = account[i] - '0';
        if (i % 2 == 0) {  // Even positions (1,3,5,7,9)
            int product = digit * 2;
            sum += (product >= 10) ? (product - 9) : product;
        } else {
            sum += digit;
        }
    }

    int remainder = sum % 10;
    int check_digit = (remainder == 0) ? 0 : (10 - remainder);

    int expected = account[9] - '0';  // Check digit at position 10 (index 9)
    return (check_digit == expected) ? CheckResult::OK : CheckResult::FALSE;
}

// ======================================================================
// Method 80: Multi-variant with Sachkonten exception
// ======================================================================
CheckResult CheckMethods::Method_80(const std::string& account, const std::string& blz) {
    // Sachkonten (position 3 = '9') always valid
    if (account[2] == '9') {
        return CheckResult::OK;
    }

    // Variant 1: Try Method 00 (MOD-10)
    CheckResult result = Method_00(account, blz);
    if (result == CheckResult::OK) {
        return CheckResult::OK;
    }

    // Variant 2: Try MOD-7 with same weights as Method 00
    int sum = 0;
    for (int i = 0; i < 9; i++) {
        int digit = account[i] - '0';
        int product = digit * WEIGHTS_00[i];
        if (product >= 10) {
            sum += (product / 10) + (product % 10);  // Cross-sum
        } else {
            sum += product;
        }
    }

    int remainder = sum % 7;
    int check_digit = (7 - remainder) % 7;
    int expected = account[9] - '0';
    return (check_digit == expected) ? CheckResult::OK : CheckResult::FALSE;
}

// ======================================================================
// Method 81: MOD-11 with Sachkonten exception (like Method 51)
// ======================================================================
CheckResult CheckMethods::Method_81(const std::string& account, const std::string& blz) {
    // Sachkonten (position 3 = '9') always valid
    if (account[2] == '9') {
        return CheckResult::OK;
    }

    // MOD-11 on positions 4-9, check at position 10
    int sum = 0;
    for (int i = 0; i < 6; i++) {
        sum += (account[i + 3] - '0') * WEIGHTS_81[i];
    }

    int remainder = sum % 11;
    int check_digit = (remainder <= 1) ? 0 : (11 - remainder);

    int expected = account[9] - '0';
    return (check_digit == expected) ? CheckResult::OK : CheckResult::FALSE;
}

// ======================================================================
// Method 82: Conditional - Method 10 or Method 33
// ======================================================================
CheckResult CheckMethods::Method_82(const std::string& account, const std::string& blz) {
    // If first two digits are "00", use Method 33
    if (account[0] == '0' && account[1] == '0') {
        return Method_33(account, blz);
    }

    // Otherwise use Method 10
    return Method_10(account, blz);
}

// ======================================================================
// Method 83: Four-variant customer accounts with Sachkonten
// ======================================================================
CheckResult CheckMethods::Method_83(const std::string& account, const std::string& blz) {
    // Sachkonten (position 3 = '9') always valid
    if (account[2] == '9') {
        return CheckResult::OK;
    }

    // Variant A: MOD-10 on positions 3-10 with weights [2,3,4,5,6,7,8,9]
    int sum = 0;
    for (int i = 0; i < 8; i++) {
        sum += (account[i + 2] - '0') * WEIGHTS_83_A[i];
    }
    int remainder = sum % 10;
    if (remainder == 0) {
        return CheckResult::OK;
    }

    // Variant B: MOD-11 on positions 3-10 with weights [2,3,4,5,6,7,8,9]
    sum = 0;
    for (int i = 0; i < 8; i++) {
        sum += (account[i + 2] - '0') * WEIGHTS_83_B[i];
    }
    remainder = sum % 11;
    if (remainder == 0) {
        return CheckResult::OK;
    }

    // Variant C: MOD-7 on positions 3-10 with weights [2,3,4,5,6,7,8,9]
    sum = 0;
    for (int i = 0; i < 8; i++) {
        sum += (account[i + 2] - '0') * WEIGHTS_83_C[i];
    }
    remainder = sum % 7;
    return (remainder == 0) ? CheckResult::OK : CheckResult::FALSE;
}

// ======================================================================
// Method 84: Two-variant MOD-11/MOD-7 with Sachkonten
// ======================================================================
CheckResult CheckMethods::Method_84(const std::string& account, const std::string& blz) {
    // Sachkonten (position 3 = '9') always valid
    if (account[2] == '9') {
        return CheckResult::OK;
    }

    // Variant 1: MOD-11 on positions 4-9, check at position 10
    int sum = 0;
    for (int i = 0; i < 6; i++) {
        sum += (account[i + 3] - '0') * WEIGHTS_84_V1[i];
    }

    int remainder = sum % 11;
    int check_digit = (remainder <= 1) ? 0 : (11 - remainder);
    int expected = account[9] - '0';

    if (check_digit == expected) {
        return CheckResult::OK;
    }

    // Variant 2: MOD-7 on positions 4-9, check at position 10
    sum = 0;
    for (int i = 0; i < 6; i++) {
        sum += (account[i + 3] - '0') * WEIGHTS_84_V1[i];
    }

    remainder = sum % 7;
    check_digit = (7 - remainder) % 7;
    return (check_digit == expected) ? CheckResult::OK : CheckResult::FALSE;
}

// ======================================================================
// Method 85: Similar to Method 83 with different Sachkonten handling
// ======================================================================
CheckResult CheckMethods::Method_85(const std::string& account, const std::string& blz) {
    // Sachkonten (position 3 = '9') use only Variant C
    if (account[2] == '9') {
        int sum = 0;
        for (int i = 0; i < 8; i++) {
            sum += (account[i + 2] - '0') * WEIGHTS_85_C[i];
        }
        int remainder = sum % 7;
        return (remainder == 0) ? CheckResult::OK : CheckResult::FALSE;
    }

    // Customer accounts: Try A, then B, then C
    // Variant A: MOD-10
    int sum = 0;
    for (int i = 0; i < 8; i++) {
        sum += (account[i + 2] - '0') * WEIGHTS_85_A[i];
    }
    int remainder = sum % 10;
    if (remainder == 0) {
        return CheckResult::OK;
    }

    // Variant B: MOD-11
    sum = 0;
    for (int i = 0; i < 8; i++) {
        sum += (account[i + 2] - '0') * WEIGHTS_85_B[i];
    }
    remainder = sum % 11;
    if (remainder == 0) {
        return CheckResult::OK;
    }

    // Variant C: MOD-7
    sum = 0;
    for (int i = 0; i < 8; i++) {
        sum += (account[i + 2] - '0') * WEIGHTS_85_C[i];
    }
    remainder = sum % 7;
    return (remainder == 0) ? CheckResult::OK : CheckResult::FALSE;
}

// ======================================================================
// Method 86: Two-variant MOD-10/MOD-11 with Sachkonten
// ======================================================================
CheckResult CheckMethods::Method_86(const std::string& account, const std::string& blz) {
    // Sachkonten (position 3 = '9') always valid
    if (account[2] == '9') {
        return CheckResult::OK;
    }

    // Variant 1: Try Method 00 (MOD-10 with cross-sum)
    CheckResult result = Method_00(account, blz);
    if (result == CheckResult::OK) {
        return CheckResult::OK;
    }

    // Variant 2: Try MOD-11 with weights [4,3,2,7,6,5,4,3,2]
    int sum = 0;
    for (int i = 0; i < 9; i++) {
        sum += (account[i] - '0') * WEIGHTS_86_V2[i];
    }

    int remainder = sum % 11;
    int check_digit = (remainder <= 1) ? 0 : (11 - remainder);
    int expected = account[9] - '0';
    return (check_digit == expected) ? CheckResult::OK : CheckResult::FALSE;
}

// ======================================================================
// Method 87: Complex Pascal algorithm with fallbacks
// ======================================================================
CheckResult CheckMethods::Method_87(const std::string& account, const std::string& blz) {
    // Sachkonten (position 3 = '9') always valid
    if (account[2] == '9') {
        return CheckResult::OK;
    }

    // Complex transformation algorithm (simplified version)
    // Try Method 33 first
    CheckResult result = Method_33(account, blz);
    if (result == CheckResult::OK) {
        return CheckResult::OK;
    }

    // Fallback to MOD-7 variant
    int sum = 0;
    for (int i = 0; i < 5; i++) {
        sum += (account[i + 5] - '0') * WEIGHTS_33[i];
    }

    int remainder = sum % 7;
    int check_digit = (7 - remainder) % 7;
    int expected = account[9] - '0';
    return (check_digit == expected) ? CheckResult::OK : CheckResult::FALSE;
}

// ======================================================================
// Method 88: MOD-11 with conditional weight addition
// ======================================================================
CheckResult CheckMethods::Method_88(const std::string& account, const std::string& blz) {
    // MOD-11 on positions 1-9, check at position 10
    int sum = 0;
    for (int i = 0; i < 9; i++) {
        int digit = account[i] - '0';
        int weight = WEIGHTS_88[i];

        // If position 3 (index 2) is '9', add 1 to the weight
        if (i == 2 && digit == 9) {
            weight += 1;
        }

        sum += digit * weight;
    }

    int remainder = sum % 11;
    int check_digit = (remainder <= 1) ? 0 : (11 - remainder);

    int expected = account[9] - '0';
    return (check_digit == expected) ? CheckResult::OK : CheckResult::FALSE;
}

// ======================================================================
// Method 89: Length-based Method 10 variant
// ======================================================================
CheckResult CheckMethods::Method_89(const std::string& account, const std::string& blz) {
    // For 10-digit accounts, use Method 10
    // For 9-digit accounts (starting with '0'), also use Method 10
    return Method_10(account, blz);
}

// ======================================================================
// Method 90: Multi-variant with Sachkonten special handling
// ======================================================================
CheckResult CheckMethods::Method_90(const std::string& account, const std::string& blz) {
    // Sachkonten (position 3 = '9')
    if (account[2] == '9') {
        int sum = 9 * 8;  // Position 3 is always '9'
        for (int i = 0; i < 6; i++) {
            sum += (account[i + 3] - '0') * WEIGHTS_90_SACH[i + 1];
        }
        int remainder = sum % 11;
        int check_digit = (remainder <= 1) ? 0 : (11 - remainder);
        int expected = account[9] - '0';
        return (check_digit == expected) ? CheckResult::OK : CheckResult::FALSE;
    }

    // Variant A: Method 32 (MOD-11 on positions 4-9)
    int sum = 0;
    for (int i = 0; i < 6; i++) {
        sum += (account[i + 3] - '0') * WEIGHTS_90_A[i];
    }
    int remainder = sum % 11;
    int check_digit = (remainder <= 1) ? 0 : (11 - remainder);
    int expected = account[9] - '0';
    if (check_digit == expected) {
        return CheckResult::OK;
    }

    // Variant B: MOD-11 on positions 5-9
    sum = 0;
    for (int i = 0; i < 5; i++) {
        sum += (account[i + 4] - '0') * WEIGHTS_90_B[i];
    }
    remainder = sum % 11;
    check_digit = (remainder <= 1) ? 0 : (11 - remainder);
    if (check_digit == expected) {
        return CheckResult::OK;
    }

    // Variant C: MOD-7 on positions 5-9
    sum = 0;
    for (int i = 0; i < 5; i++) {
        sum += (account[i + 4] - '0') * WEIGHTS_90_B[i];
    }
    remainder = sum % 7;
    check_digit = (remainder == 0) ? 0 : (7 - remainder);
    if (check_digit == expected) {
        return CheckResult::OK;
    }

    // Variant D: MOD-9 on positions 5-9
    sum = 0;
    for (int i = 0; i < 5; i++) {
        sum += (account[i + 4] - '0') * WEIGHTS_90_B[i];
    }
    remainder = sum % 9;
    check_digit = (remainder == 0) ? 0 : (9 - remainder);
    if (check_digit == expected) {
        return CheckResult::OK;
    }

    // Variant E: MOD-10 on positions 5-9
    sum = 0;
    for (int i = 0; i < 5; i++) {
        sum += (account[i + 4] - '0') * WEIGHTS_90_E[i];
    }
    remainder = sum % 10;
    check_digit = (remainder == 0) ? 0 : (10 - remainder);
    return (check_digit == expected) ? CheckResult::OK : CheckResult::FALSE;
}

// ======================================================================
// Method 91: Four-variant with check digit at position 7
// ======================================================================
CheckResult CheckMethods::Method_91(const std::string& account, const std::string& blz) {
    int expected = account[6] - '0';  // Check digit at position 7 (index 6)

    // Variant A: MOD-11 weights [7,6,5,4,3,2]
    int sum = 0;
    for (int i = 0; i < 6; i++) {
        sum += (account[i] - '0') * WEIGHTS_91_A[i];
    }
    int remainder = sum % 11;
    int check_digit = (remainder <= 1) ? 0 : (11 - remainder);
    if (check_digit == expected) {
        return CheckResult::OK;
    }

    // Variant B: MOD-11 weights [2,3,4,5,6,7]
    sum = 0;
    for (int i = 0; i < 6; i++) {
        sum += (account[i] - '0') * WEIGHTS_91_B[i];
    }
    remainder = sum % 11;
    check_digit = (remainder <= 1) ? 0 : (11 - remainder);
    if (check_digit == expected) {
        return CheckResult::OK;
    }

    // Variant C: MOD-11 weights [10,9,8,7,6,5,_skip_,4,3,2] (skip position 7)
    sum = (account[0] - '0') * 10
        + (account[1] - '0') * 9
        + (account[2] - '0') * 8
        + (account[3] - '0') * 7
        + (account[4] - '0') * 6
        + (account[5] - '0') * 5
        + (account[7] - '0') * 4
        + (account[8] - '0') * 3
        + (account[9] - '0') * 2;
    remainder = sum % 11;
    check_digit = (remainder <= 1) ? 0 : (11 - remainder);
    if (check_digit == expected) {
        return CheckResult::OK;
    }

    // Variant D: MOD-11 weights [9,10,5,8,4,2]
    sum = 0;
    for (int i = 0; i < 6; i++) {
        sum += (account[i] - '0') * WEIGHTS_91_D[i];
    }
    remainder = sum % 11;
    check_digit = (remainder <= 1) ? 0 : (11 - remainder);
    return (check_digit == expected) ? CheckResult::OK : CheckResult::FALSE;
}

// ======================================================================
// Method 92: MOD-10 on positions 4-9 with specific weights
// ======================================================================
CheckResult CheckMethods::Method_92(const std::string& account, const std::string& blz) {
    int sum = 0;
    for (int i = 0; i < 6; i++) {
        sum += (account[i + 3] - '0') * WEIGHTS_92[i];
    }
    int remainder = sum % 10;
    int check_digit = (remainder == 0) ? 0 : (10 - remainder);
    int expected = account[9] - '0';
    return (check_digit == expected) ? CheckResult::OK : CheckResult::FALSE;
}

// ======================================================================
// Method 93: Two-location variant with MOD-11/MOD-7
// ======================================================================
CheckResult CheckMethods::Method_93(const std::string& account, const std::string& blz) {
    int sum = 0;
    int check_pos;

    // Check if first 4 digits are "0000" - use positions 5-9, check at position 10
    if (account[0] == '0' && account[1] == '0' && account[2] == '0' && account[3] == '0') {
        for (int i = 0; i < 5; i++) {
            sum += (account[i + 4] - '0') * WEIGHTS_93[i];
        }
        check_pos = 9;
    } else {
        // Otherwise use positions 1-5, check at position 6 (but compare to position 10)
        for (int i = 0; i < 5; i++) {
            sum += (account[i] - '0') * WEIGHTS_93[i];
        }
        check_pos = 5;  // Actually position 6, but we'll compare to position 10
    }

    // Variant 1: MOD-11
    int remainder = sum % 11;
    int check_digit = (remainder <= 1) ? 0 : (11 - remainder);
    int expected = account[check_pos] - '0';
    if (check_digit == expected) {
        return CheckResult::OK;
    }

    // Variant 2: MOD-7
    remainder = sum % 7;
    check_digit = (remainder == 0) ? 0 : (7 - remainder);
    return (check_digit == expected) ? CheckResult::OK : CheckResult::FALSE;
}

// ======================================================================
// Method 94: MOD-10 with cross-sum on positions 1-9
// ======================================================================
CheckResult CheckMethods::Method_94(const std::string& account, const std::string& blz) {
    return Method_00(account, blz);
}

// ======================================================================
// Method 95: MOD-11 with exception ranges
// ======================================================================
CheckResult CheckMethods::Method_95(const std::string& account, const std::string& blz) {
    // Exception ranges - always valid
    if ((account >= "0000000001" && account <= "0001999999") ||
        (account >= "0009000000" && account <= "0025999999") ||
        (account >= "0396000000" && account <= "0499999999") ||
        (account >= "0700000000" && account <= "0799999999")) {
        return CheckResult::OK;
    }

    // MOD-11 with weights [4,3,2,7,6,5,4,3,2]
    int sum = 0;
    for (int i = 0; i < 9; i++) {
        sum += (account[i] - '0') * WEIGHTS_95[i];
    }
    int remainder = sum % 11;
    int check_digit = (remainder <= 1) ? 0 : (11 - remainder);
    int expected = account[9] - '0';
    return (check_digit == expected) ? CheckResult::OK : CheckResult::FALSE;
}

// ======================================================================
// Method 96: Two-variant with exception range
// ======================================================================
CheckResult CheckMethods::Method_96(const std::string& account, const std::string& blz) {
    // Exception range - always valid
    if (account >= "0001300000" && account < "0099400000") {
        return CheckResult::OK;
    }

    // Variant A: Method 19 (MOD-11 weights [1,9,8,7,6,5,4,3,2])
    CheckResult result = Method_19(account, blz);
    if (result == CheckResult::OK) {
        return CheckResult::OK;
    }

    // Variant B: Method 00 (MOD-10 with cross-sum)
    return Method_00(account, blz);
}

// ======================================================================
// Method 97: Simple MOD-11 remainder check
// ======================================================================
CheckResult CheckMethods::Method_97(const std::string& account, const std::string& blz) {
    // Convert first 9 digits to integer and get MOD-11 remainder
    long long value = 0;
    for (int i = 0; i < 9; i++) {
        value = value * 10 + (account[i] - '0');
    }
    int check_digit = value % 11;
    if (check_digit == 10) {
        check_digit = 0;
    }
    int expected = account[9] - '0';
    return (check_digit == expected) ? CheckResult::OK : CheckResult::FALSE;
}

// ======================================================================
// Method 98: Two-variant with different weight sets
// ======================================================================
CheckResult CheckMethods::Method_98(const std::string& account, const std::string& blz) {
    // Variant A: MOD-10 on positions 3-9 with weights [3,7,1,3,7,1,3]
    int sum = 0;
    for (int i = 0; i < 7; i++) {
        sum += (account[i + 2] - '0') * WEIGHTS_98_A[i];
    }
    int remainder = sum % 10;
    int check_digit = (remainder == 0) ? 0 : (10 - remainder);
    int expected = account[9] - '0';
    if (check_digit == expected) {
        return CheckResult::OK;
    }

    // Variant B: Method 32 (MOD-11 on positions 4-9)
    return Method_32(account, blz);
}

// ======================================================================
// Method 99: MOD-11 with exception range
// ======================================================================
CheckResult CheckMethods::Method_99(const std::string& account, const std::string& blz) {
    // Exception range - always valid
    if (account >= "0396000000" && account < "0500000000") {
        return CheckResult::OK;
    }

    // MOD-11 with weights [4,3,2,7,6,5,4,3,2] (same as Method 95)
    int sum = 0;
    for (int i = 0; i < 9; i++) {
        sum += (account[i] - '0') * WEIGHTS_95[i];
    }
    int remainder = sum % 11;
    int check_digit = (remainder <= 1) ? 0 : (11 - remainder);
    int expected = account[9] - '0';
    return (check_digit == expected) ? CheckResult::OK : CheckResult::FALSE;
}

// ======================================================================
// Method A0 (160): MOD-11 with exception for 3-digit accounts
// ======================================================================
CheckResult CheckMethods::Method_A0(const std::string& account, const std::string& blz) {
    // Exception: 3-digit accounts (first 7 digits are '0') - always valid
    if (account.substr(0, 7) == "0000000") {
        return CheckResult::OK;
    }

    // MOD-11 on positions 5-9 with weights [10,5,8,4,2]
    int sum = 0;
    for (int i = 0; i < 5; i++) {
        sum += (account[i + 4] - '0') * WEIGHTS_A0[i];
    }
    int remainder = sum % 11;
    int check_digit = (remainder <= 1) ? 0 : (11 - remainder);
    int expected = account[9] - '0';
    return (check_digit == expected) ? CheckResult::OK : CheckResult::FALSE;
}

// ======================================================================
// Method A1 (161): Length-based validation with Method 00
// ======================================================================
CheckResult CheckMethods::Method_A1(const std::string& account, const std::string& blz) {
    // Invalid if 9 digits (starts with exactly one '0') or less than 8 digits (starts with "000")
    if ((account[0] == '0' && account[1] != '0') || (account[0] == '0' && account[1] == '0' && account[2] == '0')) {
        return CheckResult::INVALID_KTO;
    }

    // Use Method 00 on positions 3-9
    // MOD-10 with cross-sum on last 7 positions
    int sum = 0;
    for (int i = 2; i < 9; i++) {
        int digit = account[i] - '0';
        if (i % 2 == 0) {  // Even indices (positions 3,5,7,9)
            int product = digit * 2;
            sum += (product >= 10) ? (product - 9) : product;
        } else {
            sum += digit;
        }
    }
    int remainder = sum % 10;
    int check_digit = (remainder == 0) ? 0 : (10 - remainder);
    int expected = account[9] - '0';
    return (check_digit == expected) ? CheckResult::OK : CheckResult::FALSE;
}

// ======================================================================
// Method A2 (162): Two-variant Method 00 → Method 04
// ======================================================================
CheckResult CheckMethods::Method_A2(const std::string& account, const std::string& blz) {
    // Variant 1: Try Method 00
    CheckResult result = Method_00(account, blz);
    if (result == CheckResult::OK) {
        return CheckResult::OK;
    }

    // Variant 2: Try Method 04 (MOD-11)
    int sum = 0;
    for (int i = 0; i < 9; i++) {
        sum += (account[i] - '0') * WEIGHTS_04[i];
    }
    int remainder = sum % 11;
    if (remainder != 0) {
        remainder = 11 - remainder;
    }
    if (remainder == 10) {
        return CheckResult::INVALID_KTO;
    }
    int expected = account[9] - '0';
    return (remainder == expected) ? CheckResult::OK : CheckResult::FALSE;
}

// ======================================================================
// Method A3 (163): Two-variant Method 00 → Method 10
// ======================================================================
CheckResult CheckMethods::Method_A3(const std::string& account, const std::string& blz) {
    // Variant 1: Try Method 00
    CheckResult result = Method_00(account, blz);
    if (result == CheckResult::OK) {
        return CheckResult::OK;
    }

    // Variant 2: Method 10
    return Method_10(account, blz);
}

// ======================================================================
// Method A4 (164): Four-variant with position 3-4 checking
// ======================================================================
CheckResult CheckMethods::Method_A4(const std::string& account, const std::string& blz) {
    // Positions 3-4 (indices 2-3) == "99" - use variants 3 then 4
    if (account[2] == '9' && account[3] == '9') {
        // Variant 3: MOD-11 on positions 5-9
        int sum = 0;
        for (int i = 0; i < 5; i++) {
            sum += (account[i + 4] - '0') * WEIGHTS_93[i];
        }
        int remainder = sum % 11;
        int check_digit = (remainder <= 1) ? 0 : (11 - remainder);
        int expected = account[9] - '0';
        if (check_digit == expected) {
            return CheckResult::OK;
        }
        // Fall through to variant 4 (Method 93)
    } else {
        // Variant 1: MOD-11 on positions 4-9
        int sum = 0;
        for (int i = 0; i < 6; i++) {
            sum += (account[i + 3] - '0') * WEIGHTS_90_A[i];
        }
        int remainder = sum % 11;
        int check_digit = (remainder <= 1) ? 0 : (11 - remainder);
        int expected = account[9] - '0';
        if (check_digit == expected) {
            return CheckResult::OK;
        }

        // Variant 2: MOD-7 on positions 4-9
        sum = 0;
        for (int i = 0; i < 6; i++) {
            sum += (account[i + 3] - '0') * WEIGHTS_90_A[i];
        }
        remainder = sum % 7;
        check_digit = (remainder == 0) ? 0 : (7 - remainder);
        if (check_digit == expected) {
            return CheckResult::OK;
        }
    }

    // Variant 4: Method 93
    return Method_93(account, blz);
}

// ======================================================================
// Method A5 (165): Method 00 → invalid if first digit is 9, else Method 10
// ======================================================================
CheckResult CheckMethods::Method_A5(const std::string& account, const std::string& blz) {
    // Try Method 00 first
    CheckResult result = Method_00(account, blz);
    if (result == CheckResult::OK) {
        return CheckResult::OK;
    }

    // If first digit is '9', account is invalid
    if (account[0] == '9') {
        return CheckResult::INVALID_KTO;
    }

    // Otherwise try Method 10
    return Method_10(account, blz);
}

// ======================================================================
// Method A6 (166): Position 2 based selection - Method 00 or Method 01
// ======================================================================
CheckResult CheckMethods::Method_A6(const std::string& account, const std::string& blz) {
    // If position 2 (index 1) is '8', use Method 00
    if (account[1] == '8') {
        return Method_00(account, blz);
    }

    // Otherwise use Method 01
    return Method_01(account, blz);
}

// ======================================================================
// Method A7 (167): Two-variant Method 00 → Method 03
// ======================================================================
CheckResult CheckMethods::Method_A7(const std::string& account, const std::string& blz) {
    // Variant 1: Try Method 00
    CheckResult result = Method_00(account, blz);
    if (result == CheckResult::OK) {
        return CheckResult::OK;
    }

    // Variant 2: Method 03 (MOD-10 without cross-sum)
    return Method_03(account, blz);
}

// ======================================================================
// Method A8 (168): Sachkonten exception with two variants
// ======================================================================
CheckResult CheckMethods::Method_A8(const std::string& account, const std::string& blz) {
    // Sachkonten (position 3 = '9') - use method from Method 51
    if (account[2] == '9') {
        // Exception variant 1: MOD-11 on positions 3-9
        int sum = 9 * 8;
        for (int i = 0; i < 6; i++) {
            sum += (account[i + 3] - '0') * WEIGHTS_A8_V1[i];
        }
        int remainder = sum % 11;
        int check_digit = (remainder <= 1) ? 0 : (11 - remainder);
        int expected = account[9] - '0';
        if (check_digit == expected) {
            return CheckResult::OK;
        }

        // Exception variant 2: Method 10
        return Method_10(account, blz);
    }

    // Variant 1: Method 32 (MOD-11 on positions 4-9)
    CheckResult result = Method_32(account, blz);
    if (result == CheckResult::OK) {
        return CheckResult::OK;
    }

    // Variant 2: MOD-10 with cross-sum on positions 4-9
    int sum = 0;
    for (int i = 3; i < 9; i++) {
        int digit = account[i] - '0';
        if (i % 2 == 1) {  // Odd indices (positions 4,6,8)
            int product = digit * 2;
            sum += (product >= 10) ? (product - 9) : product;
        } else {
            sum += digit;
        }
    }
    int remainder = sum % 10;
    int check_digit = (remainder == 0) ? 0 : (10 - remainder);
    int expected = account[9] - '0';
    return (check_digit == expected) ? CheckResult::OK : CheckResult::FALSE;
}

// ======================================================================
// Method A9 (169): Two-variant Method 01 → Method 06
// ======================================================================
CheckResult CheckMethods::Method_A9(const std::string& account, const std::string& blz) {
    // Variant 1: Try Method 01
    CheckResult result = Method_01(account, blz);
    if (result == CheckResult::OK) {
        return CheckResult::OK;
    }

    // Variant 2: Method 06 (same as Method 04)
    return Method_06(account, blz);
}

// ======================================================================
// Method B0 (176): Position 1 and 8 validation with conditional logic
// ======================================================================
CheckResult CheckMethods::Method_B0(const std::string& account, const std::string& blz) {
    // Invalid if first digit is '0' or '8'
    if (account[0] == '0' || account[0] == '8') {
        return CheckResult::INVALID_KTO;
    }

    // If position 8 (index 7) is 1, 2, 3, or 6 - no check (always valid)
    if (account[7] == '1' || account[7] == '2' || account[7] == '3' || account[7] == '6') {
        return CheckResult::OK;
    }

    // Otherwise use Method 06
    return Method_06(account, blz);
}

// ======================================================================
// Method B1 (177): Two-variant Method 05 → Method 01
// ======================================================================
CheckResult CheckMethods::Method_B1(const std::string& account, const std::string& blz) {
    // Variant 1: Try Method 05
    CheckResult result = Method_05(account, blz);
    if (result == CheckResult::OK) {
        return CheckResult::OK;
    }

    // Variant 2: Method 01
    return Method_01(account, blz);
}

// ======================================================================
// Method B2 (178): First digit based selection - Method 02 or Method 00
// ======================================================================
CheckResult CheckMethods::Method_B2(const std::string& account, const std::string& blz) {
    // If first digit is 0-7, use Method 02
    if (account[0] < '8') {
        return Method_02(account, blz);
    }

    // Otherwise (8-9) use Method 00
    return Method_00(account, blz);
}

// ======================================================================
// Method B3 (179): First digit based selection - Method 32 or Method 06
// ======================================================================
CheckResult CheckMethods::Method_B3(const std::string& account, const std::string& blz) {
    // If first digit is 0-8, use Method 32
    if (account[0] < '9') {
        return Method_32(account, blz);
    }

    // Otherwise (9) use Method 06
    return Method_06(account, blz);
}

// ======================================================================
// Method B4 (180): First digit based selection - Method 00 or Method 02
// ======================================================================
CheckResult CheckMethods::Method_B4(const std::string& account, const std::string& blz) {
    // If first digit is 9, use Method 00
    if (account[0] == '9') {
        return Method_00(account, blz);
    }

    // Otherwise (0-8) use Method 02
    return Method_02(account, blz);
}

// ======================================================================
// Method B5 (181): Method 05 → reject if first digit 8-9, else Method 00
// ======================================================================
CheckResult CheckMethods::Method_B5(const std::string& account, const std::string& blz) {
    // Try Method 05 first
    CheckResult result = Method_05(account, blz);
    if (result == CheckResult::OK) {
        return CheckResult::OK;
    }

    // If first digit is 8 or 9, reject
    if (account[0] > '7') {
        return CheckResult::FALSE;
    }

    // Otherwise try Method 00
    return Method_00(account, blz);
}

// ======================================================================
// Method B6 (182): First digit based selection - Method 20 or ESER (Method 53)
// ======================================================================
CheckResult CheckMethods::Method_B6(const std::string& account, const std::string& blz) {
    // If first digit is 1-9, use Method 20
    if (account[0] > '0') {
        return Method_20(account, blz);
    }

    // Otherwise (0) use Method 53 (ESER) - return NOT_IMPLEMENTED for now
    // as Method 53 requires BLZ and complex ESER transformation
    return CheckResult::NOT_IMPLEMENTED;
}

// ======================================================================
// Method B7 (183): Range-based validation - Method 01 or no check
// ======================================================================
CheckResult CheckMethods::Method_B7(const std::string& account, const std::string& blz) {
    // Ranges that require Method 01 validation:
    // 0001000000-0005999999 and 0700000000-0899999999
    bool in_range = (account >= "0001000000" && account <= "0005999999") ||
                    (account >= "0700000000" && account <= "0899999999");

    if (in_range) {
        return Method_01(account, blz);
    }

    // All other accounts - no check (always valid)
    return CheckResult::OK;
}

// ======================================================================
// Method B8 (184): Two-variant Method 20 → Method 29 (M10H)
// ======================================================================
CheckResult CheckMethods::Method_B8(const std::string& account, const std::string& blz) {
    // Variant 1: Try Method 20
    CheckResult result = Method_20(account, blz);
    if (result == CheckResult::OK) {
        return CheckResult::OK;
    }

    // Variant 2: Method 29 (M10H transformation)
    return Method_29(account, blz);
}

// ======================================================================
// Method B9 (185): Complex two/three leading zeros validation
// ======================================================================
CheckResult CheckMethods::Method_B9(const std::string& account, const std::string& blz) {
    // Must have exactly 2 or 3 leading zeros
    if (account[0] != '0' || account[1] != '0') {
        return CheckResult::INVALID_KTO;
    }
    if (account[0] == '0' && account[1] == '0' && account[2] == '0' && account[3] == '0') {
        return CheckResult::INVALID_KTO;
    }

    // Exactly 2 leading zeros (3rd position != '0')
    if (account[2] != '0') {
        // Variant 1: Complex MOD-(11,10) calculation
        int sum = 0;
        int weights[] = {1, 2, 3, 1, 2, 3, 1};
        for (int i = 0; i < 7; i++) {
            int digit = account[i + 2] - '0';
            int product = digit * weights[i] + weights[i];
            sum += product % 11;
        }
        int remainder = sum % 10;
        int expected = account[9] - '0';
        if (remainder == expected) {
            return CheckResult::OK;
        }
        // Try with +5
        int check_digit = (remainder + 5) % 10;
        return (check_digit == expected) ? CheckResult::OK : CheckResult::FALSE;
    }

    // Exactly 3 leading zeros (positions 4-9 used)
    // Variant 2: MOD-11 on positions 4-9
    int sum = 0;
    for (int i = 0; i < 6; i++) {
        sum += (account[i + 3] - '0') * WEIGHTS_B9_V2[i];
    }
    int remainder = sum % 11;
    int expected = account[9] - '0';
    if (remainder == expected) {
        return CheckResult::OK;
    }
    // Try with +5
    int check_digit = (remainder + 5) % 10;
    return (check_digit == expected) ? CheckResult::OK : CheckResult::FALSE;
}

// ======================================================================
// Method C0 (192): Two leading zeros check - ESER or Method 20
// ======================================================================
CheckResult CheckMethods::Method_C0(const std::string& account, const std::string& blz) {
    // Accounts with exactly 2 leading zeros - try ESER (Method 52) then Method 20
    if (account[0] == '0' && account[1] == '0') {
        // ESER variant requires BLZ - return NOT_IMPLEMENTED
        // Fall through to Method 20
    }

    // All others, or fallback: Method 20
    return Method_20(account, blz);
}

// ======================================================================
// Method C1 (193): First digit based selection
// ======================================================================
CheckResult CheckMethods::Method_C1(const std::string& account, const std::string& blz) {
    // If first digit is not '5', use Method 17
    if (account[0] != '5') {
        return Method_17(account, blz);
    }

    // If first digit is '5', special cross-sum MOD-11 calculation
    int sum = 0;
    for (int i = 0; i < 9; i++) {
        int digit = account[i] - '0';
        if (i % 2 == 1) {  // Odd indices (positions 2,4,6,8)
            int product = digit * 2;
            sum += (product >= 10) ? (product - 9) : product;
        } else {
            sum += digit;
        }
    }
    sum -= 1;  // Subtract 1 from sum
    int remainder = sum % 11;
    int check_digit = (remainder == 0) ? 0 : (10 - remainder);
    int expected = account[9] - '0';
    return (check_digit == expected) ? CheckResult::OK : CheckResult::FALSE;
}

// ======================================================================
// Method C2 (194): Two-variant Method 22 → Method 00
// ======================================================================
CheckResult CheckMethods::Method_C2(const std::string& account, const std::string& blz) {
    // Variant 1: Try Method 22
    CheckResult result = Method_22(account, blz);
    if (result == CheckResult::OK) {
        return CheckResult::OK;
    }

    // Variant 2: Method 00
    return Method_00(account, blz);
}

// ======================================================================
// Method C3 (195): First digit based selection - Method 00 or Method 58
// ======================================================================
CheckResult CheckMethods::Method_C3(const std::string& account, const std::string& blz) {
    // If first digit is not '9', use Method 00
    if (account[0] != '9') {
        return Method_00(account, blz);
    }

    // If first digit is '9', use Method 58
    return Method_58(account, blz);
}

// ======================================================================
// Method C4 (196): First digit based selection - Method 15 or Method 58
// ======================================================================
CheckResult CheckMethods::Method_C4(const std::string& account, const std::string& blz) {
    // If first digit is not '9', use Method 15
    if (account[0] != '9') {
        return Method_15(account, blz);
    }

    // If first digit is '9', use Method 58
    return Method_58(account, blz);
}

// ======================================================================
// Method C5 (197): Complex multi-range validation
// ======================================================================
CheckResult CheckMethods::Method_C5(const std::string& account, const std::string& blz) {
    // Variant 1a: 6-digit accounts (positions 1-4 are '0000', position 5 is 1-8)
    if (account.substr(0, 4) == "0000" && account[4] >= '1' && account[4] <= '8') {
        // Method 75: MOD-10 on positions 5-9
        return Method_75(account, blz);
    }

    // Variant 1b: 9-digit accounts (position 1 is '0', position 2 is 1-8)
    if (account[0] == '0' && account[1] >= '1' && account[1] <= '8') {
        // Method 75 starting from position 2
        int sum = 0;
        for (int i = 1; i < 6; i++) {
            int digit = account[i] - '0';
            if (i % 2 == 1) {  // Odd indices
                int product = digit * 2;
                sum += (product >= 10) ? (product - 9) : product;
            } else {
                sum += digit;
            }
        }
        int remainder = sum % 10;
        int check_digit = (remainder == 0) ? 0 : (10 - remainder);
        int expected = account[6] - '0';
        return (check_digit == expected) ? CheckResult::OK : CheckResult::FALSE;
    }

    // Variant 2: 10-digit accounts, first digit is 1, 4, 5, 6, or 9
    if (account[0] == '1' || (account[0] >= '4' && account[0] <= '6') || account[0] == '9') {
        // Method 29 (M10H transformation)
        return Method_29(account, blz);
    }

    // Variant 3: 10-digit accounts, first digit is 3
    if (account[0] == '3') {
        return Method_00(account, blz);
    }

    // Variant 4: No check ranges
    // 8-digit (positions 1-2 are "00", position 3 is 3,4,5)
    if (account[0] == '0' && account[1] == '0' && account[2] >= '3' && account[2] <= '5') {
        return CheckResult::OK;
    }
    // 10-digit (positions 1-2 are "70" or "85")
    if ((account[0] == '7' && account[1] == '0') || (account[0] == '8' && account[1] == '5')) {
        return CheckResult::OK;
    }

    // Account doesn't match any valid range
    return CheckResult::INVALID_KTO;
}

// ======================================================================
// Method C6 (198): Method 00 with constant prefix "5499570"
// ======================================================================
CheckResult CheckMethods::Method_C6(const std::string& account, const std::string& blz) {
    // The constant "5499570" contributes a fixed value of 31 to the cross-sum
    // So we start with sum = 31 and process positions 2-9
    int sum = 31;  // Pre-calculated cross-sum contribution from constant

    for (int i = 1; i < 9; i++) {
        int digit = account[i] - '0';
        if (i % 2 == 1) {  // Odd indices (positions 2,4,6,8)
            int product = digit * 2;
            sum += (product >= 10) ? (product - 9) : product;
        } else {
            sum += digit;
        }
    }

    int remainder = sum % 10;
    int check_digit = (remainder == 0) ? 0 : (10 - remainder);
    int expected = account[9] - '0';
    return (check_digit == expected) ? CheckResult::OK : CheckResult::FALSE;
}

} // namespace kontocheck
} // namespace stps
} // namespace duckdb
