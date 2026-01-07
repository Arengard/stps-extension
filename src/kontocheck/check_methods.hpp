#pragma once

#include <string>
#include <cstdint>

namespace duckdb {
namespace stps {
namespace kontocheck {

// Result codes for account validation
enum class CheckResult {
    OK = 1,              // Valid account number
    FALSE = 0,           // Invalid check digit
    INVALID_KTO = -3,    // Invalid account number format
    NOT_IMPLEMENTED = -2 // Method not yet implemented
};

// German bank account check digit validation methods
// Based on Deutsche Bundesbank Prüfzifferberechnungsmethoden (Juni 2018)
class CheckMethods {
public:
    // Main entry point - validate account number using specified method
    static CheckResult ValidateAccount(
        const std::string& account,
        uint8_t method_id,
        const std::string& blz);

private:
    // Helper functions for common patterns
    static CheckResult ModulusCheck(
        const std::string& account,
        const int* weights,
        int weight_count,
        int modulus,
        bool use_cross_sum = false);

    // Methods 00-09
    static CheckResult Method_00(const std::string& account, const std::string& blz);
    static CheckResult Method_01(const std::string& account, const std::string& blz);
    static CheckResult Method_02(const std::string& account, const std::string& blz);
    static CheckResult Method_03(const std::string& account, const std::string& blz);
    static CheckResult Method_04(const std::string& account, const std::string& blz);
    static CheckResult Method_05(const std::string& account, const std::string& blz);
    static CheckResult Method_06(const std::string& account, const std::string& blz);
    static CheckResult Method_07(const std::string& account, const std::string& blz);
    static CheckResult Method_08(const std::string& account, const std::string& blz);
    static CheckResult Method_09(const std::string& account, const std::string& blz);

    // Methods 10-99 (to be implemented in batches)
    static CheckResult Method_10(const std::string& account, const std::string& blz);
    static CheckResult Method_11(const std::string& account, const std::string& blz);
    static CheckResult Method_12(const std::string& account, const std::string& blz);
    static CheckResult Method_13(const std::string& account, const std::string& blz);
    static CheckResult Method_14(const std::string& account, const std::string& blz);
    static CheckResult Method_15(const std::string& account, const std::string& blz);
    static CheckResult Method_16(const std::string& account, const std::string& blz);
    static CheckResult Method_17(const std::string& account, const std::string& blz);
    static CheckResult Method_18(const std::string& account, const std::string& blz);
    static CheckResult Method_19(const std::string& account, const std::string& blz);

    // Methods 20-29
    static CheckResult Method_20(const std::string& account, const std::string& blz);
    static CheckResult Method_21(const std::string& account, const std::string& blz);
    static CheckResult Method_22(const std::string& account, const std::string& blz);
    static CheckResult Method_23(const std::string& account, const std::string& blz);
    static CheckResult Method_24(const std::string& account, const std::string& blz);
    static CheckResult Method_25(const std::string& account, const std::string& blz);
    static CheckResult Method_26(const std::string& account, const std::string& blz);
    static CheckResult Method_27(const std::string& account, const std::string& blz);
    static CheckResult Method_28(const std::string& account, const std::string& blz);
    static CheckResult Method_29(const std::string& account, const std::string& blz);

    // Methods 30-39
    static CheckResult Method_30(const std::string& account, const std::string& blz);
    static CheckResult Method_31(const std::string& account, const std::string& blz);
    static CheckResult Method_32(const std::string& account, const std::string& blz);
    static CheckResult Method_33(const std::string& account, const std::string& blz);
    static CheckResult Method_34(const std::string& account, const std::string& blz);
    static CheckResult Method_35(const std::string& account, const std::string& blz);
    static CheckResult Method_36(const std::string& account, const std::string& blz);
    static CheckResult Method_37(const std::string& account, const std::string& blz);
    static CheckResult Method_38(const std::string& account, const std::string& blz);
    static CheckResult Method_39(const std::string& account, const std::string& blz);

    // Methods 40-49
    static CheckResult Method_40(const std::string& account, const std::string& blz);
    static CheckResult Method_41(const std::string& account, const std::string& blz);
    static CheckResult Method_42(const std::string& account, const std::string& blz);
    static CheckResult Method_43(const std::string& account, const std::string& blz);
    static CheckResult Method_44(const std::string& account, const std::string& blz);
    static CheckResult Method_45(const std::string& account, const std::string& blz);
    static CheckResult Method_46(const std::string& account, const std::string& blz);
    static CheckResult Method_47(const std::string& account, const std::string& blz);
    static CheckResult Method_48(const std::string& account, const std::string& blz);
    static CheckResult Method_49(const std::string& account, const std::string& blz);

    // Methods 50-59
    static CheckResult Method_50(const std::string& account, const std::string& blz);
    static CheckResult Method_51(const std::string& account, const std::string& blz);
    static CheckResult Method_52(const std::string& account, const std::string& blz);
    static CheckResult Method_53(const std::string& account, const std::string& blz);
    static CheckResult Method_54(const std::string& account, const std::string& blz);
    static CheckResult Method_55(const std::string& account, const std::string& blz);
    static CheckResult Method_56(const std::string& account, const std::string& blz);
    static CheckResult Method_57(const std::string& account, const std::string& blz);
    static CheckResult Method_58(const std::string& account, const std::string& blz);
    static CheckResult Method_59(const std::string& account, const std::string& blz);

    // Methods 60-69
    static CheckResult Method_60(const std::string& account, const std::string& blz);
    static CheckResult Method_61(const std::string& account, const std::string& blz);
    static CheckResult Method_62(const std::string& account, const std::string& blz);
    static CheckResult Method_63(const std::string& account, const std::string& blz);
    static CheckResult Method_64(const std::string& account, const std::string& blz);
    static CheckResult Method_65(const std::string& account, const std::string& blz);
    static CheckResult Method_66(const std::string& account, const std::string& blz);
    static CheckResult Method_67(const std::string& account, const std::string& blz);
    static CheckResult Method_68(const std::string& account, const std::string& blz);
    static CheckResult Method_69(const std::string& account, const std::string& blz);

    // Methods 70-79
    static CheckResult Method_70(const std::string& account, const std::string& blz);
    static CheckResult Method_71(const std::string& account, const std::string& blz);
    static CheckResult Method_72(const std::string& account, const std::string& blz);
    static CheckResult Method_73(const std::string& account, const std::string& blz);
    static CheckResult Method_74(const std::string& account, const std::string& blz);
    static CheckResult Method_75(const std::string& account, const std::string& blz);
    static CheckResult Method_76(const std::string& account, const std::string& blz);
    static CheckResult Method_77(const std::string& account, const std::string& blz);
    static CheckResult Method_78(const std::string& account, const std::string& blz);
    static CheckResult Method_79(const std::string& account, const std::string& blz);

    // Methods 80-89
    static CheckResult Method_80(const std::string& account, const std::string& blz);
    static CheckResult Method_81(const std::string& account, const std::string& blz);
    static CheckResult Method_82(const std::string& account, const std::string& blz);
    static CheckResult Method_83(const std::string& account, const std::string& blz);
    static CheckResult Method_84(const std::string& account, const std::string& blz);
    static CheckResult Method_85(const std::string& account, const std::string& blz);
    static CheckResult Method_86(const std::string& account, const std::string& blz);
    static CheckResult Method_87(const std::string& account, const std::string& blz);
    static CheckResult Method_88(const std::string& account, const std::string& blz);
    static CheckResult Method_89(const std::string& account, const std::string& blz);

    // Methods 90-99
    static CheckResult Method_90(const std::string& account, const std::string& blz);
    static CheckResult Method_91(const std::string& account, const std::string& blz);
    static CheckResult Method_92(const std::string& account, const std::string& blz);
    static CheckResult Method_93(const std::string& account, const std::string& blz);
    static CheckResult Method_94(const std::string& account, const std::string& blz);
    static CheckResult Method_95(const std::string& account, const std::string& blz);
    static CheckResult Method_96(const std::string& account, const std::string& blz);
    static CheckResult Method_97(const std::string& account, const std::string& blz);
    static CheckResult Method_98(const std::string& account, const std::string& blz);
    static CheckResult Method_99(const std::string& account, const std::string& blz);

    // Methods A0-A9 (160-169)
    static CheckResult Method_A0(const std::string& account, const std::string& blz);
    static CheckResult Method_A1(const std::string& account, const std::string& blz);
    static CheckResult Method_A2(const std::string& account, const std::string& blz);
    static CheckResult Method_A3(const std::string& account, const std::string& blz);
    static CheckResult Method_A4(const std::string& account, const std::string& blz);
    static CheckResult Method_A5(const std::string& account, const std::string& blz);
    static CheckResult Method_A6(const std::string& account, const std::string& blz);
    static CheckResult Method_A7(const std::string& account, const std::string& blz);
    static CheckResult Method_A8(const std::string& account, const std::string& blz);
    static CheckResult Method_A9(const std::string& account, const std::string& blz);

    // Methods B0-B9 (176-185)
    static CheckResult Method_B0(const std::string& account, const std::string& blz);
    static CheckResult Method_B1(const std::string& account, const std::string& blz);
    static CheckResult Method_B2(const std::string& account, const std::string& blz);
    static CheckResult Method_B3(const std::string& account, const std::string& blz);
    static CheckResult Method_B4(const std::string& account, const std::string& blz);
    static CheckResult Method_B5(const std::string& account, const std::string& blz);
    static CheckResult Method_B6(const std::string& account, const std::string& blz);
    static CheckResult Method_B7(const std::string& account, const std::string& blz);
    static CheckResult Method_B8(const std::string& account, const std::string& blz);
    static CheckResult Method_B9(const std::string& account, const std::string& blz);

    // Methods C0-C6 (192-198)
    static CheckResult Method_C0(const std::string& account, const std::string& blz);
    static CheckResult Method_C1(const std::string& account, const std::string& blz);
    static CheckResult Method_C2(const std::string& account, const std::string& blz);
    static CheckResult Method_C3(const std::string& account, const std::string& blz);
    static CheckResult Method_C4(const std::string& account, const std::string& blz);
    static CheckResult Method_C5(const std::string& account, const std::string& blz);
    static CheckResult Method_C6(const std::string& account, const std::string& blz);
    // ... more methods will be added

    // Weight tables (const arrays stored in .cpp file)
    static const int WEIGHTS_00[9];
    static const int WEIGHTS_01[9];
    static const int WEIGHTS_02[9];
    static const int WEIGHTS_04[9];
    static const int WEIGHTS_05[9];
    static const int WEIGHTS_06[9];
    static const int WEIGHTS_07[9];
    static const int WEIGHTS_10[9];
    static const int WEIGHTS_14[6];
    static const int WEIGHTS_15[4];
    static const int WEIGHTS_16[9];
    static const int WEIGHTS_17[6];
    static const int WEIGHTS_18[9];
    static const int WEIGHTS_19[9];
    static const int WEIGHTS_20[9];
    static const int WEIGHTS_23[6];
    static const int WEIGHTS_24[9];
    static const int WEIGHTS_25[8];
    static const int WEIGHTS_26_V1[7];
    static const int WEIGHTS_26_V2[7];
    static const int WEIGHTS_30[5];
    static const int WEIGHTS_31[9];
    static const int WEIGHTS_32[6];
    static const int WEIGHTS_33[5];
    static const int WEIGHTS_34[7];
    static const int WEIGHTS_35[9];
    static const int WEIGHTS_36[4];
    static const int WEIGHTS_37[5];
    static const int WEIGHTS_38[6];
    static const int WEIGHTS_39[7];
    static const int WEIGHTS_40[9];
    static const int WEIGHTS_42[8];
    static const int WEIGHTS_43[9];
    static const int WEIGHTS_50[6];
    static const int WEIGHTS_51_A[6];
    static const int WEIGHTS_51_B[5];
    static const int WEIGHTS_51_EX1[7];
    static const int WEIGHTS_51_EX2[9];
    static const int WEIGHTS_54[7];
    static const int WEIGHTS_55[9];
    static const int WEIGHTS_56[9];
    static const int WEIGHTS_58[5];
    static const int WEIGHTS_64[6];
    static const int WEIGHTS_66[6];
    static const int WEIGHTS_69[7];
    static const int WEIGHTS_70_V1[9];
    static const int WEIGHTS_70_V2[6];
    static const int WEIGHTS_70_V3[8];
    static const int WEIGHTS_71[6];
    static const int WEIGHTS_76[6];
    static const int WEIGHTS_77_V1[5];
    static const int WEIGHTS_77_V2[5];
    static const int WEIGHTS_81[6];
    static const int WEIGHTS_83_A[8];
    static const int WEIGHTS_83_B[8];
    static const int WEIGHTS_83_C[8];
    static const int WEIGHTS_84_V1[6];
    static const int WEIGHTS_85_A[8];
    static const int WEIGHTS_85_B[8];
    static const int WEIGHTS_85_C[8];
    static const int WEIGHTS_86_V1[9];
    static const int WEIGHTS_86_V2[9];
    static const int WEIGHTS_88[9];
    static const int WEIGHTS_90_SACH[7];
    static const int WEIGHTS_90_A[6];
    static const int WEIGHTS_90_B[5];
    static const int WEIGHTS_90_E[5];
    static const int WEIGHTS_91_A[6];
    static const int WEIGHTS_91_B[6];
    static const int WEIGHTS_91_C[9];
    static const int WEIGHTS_91_D[6];
    static const int WEIGHTS_92[6];
    static const int WEIGHTS_93[5];
    static const int WEIGHTS_95[9];
    static const int WEIGHTS_98_A[7];
    static const int WEIGHTS_A0[5];
    static const int WEIGHTS_A6[9];
    static const int WEIGHTS_A8_V1[6];
    static const int WEIGHTS_B9_V2[6];

    // M10H transformation table for iterative transformation methods
    static const int M10H_DIGITS[4][10];
};

} // namespace kontocheck
} // namespace stps
} // namespace duckdb
