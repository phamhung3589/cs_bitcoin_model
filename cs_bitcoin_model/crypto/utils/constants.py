from enum import Enum


datetime = "datetime"
ind_mpi = "ind_mpi"
ind_ex_net = "ind_exchange_netflow"
ind_ratio_usd = "ind_stablecoins_ratio_usd"
ind_leverage_ratio = "ind_leverage_ratio"
ind_sopr = "ind_sopr"
ind_sopr_holders = "ind_sopr_holders"
ind_mvrv = "ind_mvrv"
ind_nupl = "ind_nupl"
ind_nvt_golden_cross = "ind_nvt_golden_cross"
ind_puell = "ind_puell_multiple"
ind_utxo = "ind_utxo"
ind_long_liquidation = "ind_long_liquidation"
ind_short_liquidation = "ind_short_liquidation"

list_indicators = [datetime, ind_mpi, ind_ex_net, ind_ratio_usd, ind_leverage_ratio, ind_sopr, ind_sopr_holders, ind_mvrv,
                   ind_nupl, ind_nvt_golden_cross, ind_puell, ind_utxo, ind_long_liquidation, ind_short_liquidation]


class Coefficient(str, Enum):
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    VERY_HIGH = "VERY_HIGH"


class ValueImportance(str, Enum):
    EXCELLENT = "EXCELLENT"
    GOOD = "GOOD"
    AVERAGE = "AVERAGE"
    RISKY = "RISKY"
    DANGEROUS = "DANGEROUS"


class Constants:
    JWT_SECRET = "hzFSok2hC4NVaYePMpOF0oZKpiIq8OcDY3ixGESyckhy2u2FzuEeO3U7wE59u7DP"
    JWT_ALGORITHM = "HS256"

list_indicators_name = ("ind_mpi",
                   "ind_exchange_netflow",
                   "ind_stablecoins_ratio_usd",
                   "ind_leverage_ratio",
                   "ind_sopr",
                   "ind_sopr_holders",
                   "ind_mvrv",
                   "ind_nupl",
                   "ind_nvt_golden_cross",
                   "ind_puell_multiple",
                   "ind_utxo",
                   "ind_long_liquidation",
                   "ind_short_liquidation")