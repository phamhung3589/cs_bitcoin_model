from cs_bitcoin_model.crypto.utils.constants import ValueImportance
from cs_bitcoin_model.crypto.utils.cs_config import CSConfig


class ClassifyIndicatorLiquidation:
    def __init__(self):
        parser = CSConfig("production", self.__class__.__name__, "config")
        self.short_excellent_thres = float(parser.read_parameter("short_excellent_thres"))
        self.short_good_thres = float(parser.read_parameter("short_good_thres"))
        self.short_average_thres = float(parser.read_parameter("short_average_thres"))
        self.long_average_thres = float(parser.read_parameter("long_average_thres"))
        self.long_risky_thres = float(parser.read_parameter("long_risky_thres"))
        self.long_dangerous_thres = float(parser.read_parameter("long_dangerous_thres"))
        self.long_coefficient = parser.read_parameter("long_coefficient")
        self.short_coefficient = parser.read_parameter("short_coefficient")

    def check_indicator(self, long_val, short_val):
        if short_val > self.short_excellent_thres:
            short_type = ValueImportance.EXCELLENT
        elif short_val > self.short_good_thres:
            short_type = ValueImportance.GOOD
        else:
            short_type = ValueImportance.AVERAGE

        if long_val < self.long_average_thres:
            long_type = ValueImportance.AVERAGE
        elif long_val < self.long_risky_thres:
            long_type = ValueImportance.RISKY
        else:
            long_type = ValueImportance.DANGEROUS

        return {"long_liq_type": long_type, "long_coef": self.long_coefficient,
                "short_liq_type": short_type, "short_coef": self.short_coefficient}
