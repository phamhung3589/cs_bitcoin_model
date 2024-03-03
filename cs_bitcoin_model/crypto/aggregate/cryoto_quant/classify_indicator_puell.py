from cs_bitcoin_model.crypto.utils.constants import ValueImportance
from cs_bitcoin_model.crypto.utils.cs_config import CSConfig


class ClassifyIndicatorPuell:
    def __init__(self):
        parser = CSConfig("production", self.__class__.__name__, "config")
        self.excellent_thres = float(parser.read_parameter("excellent_thres"))
        self.good_thres = float(parser.read_parameter("good_thres"))
        self.average_thres = float(parser.read_parameter("average_thres"))
        self.risky_thres = float(parser.read_parameter("risky_thres"))
        self.dangerous_thres = float(parser.read_parameter("dangerous_thres"))
        self.coefficient = parser.read_parameter("coefficient")

    def check_indicator(self, val):
        if val < self.excellent_thres:
            return ValueImportance.EXCELLENT, self.coefficient
        elif val < self.good_thres:
            return ValueImportance.GOOD, self.coefficient
        elif val < self.average_thres:
            return ValueImportance.AVERAGE, self.coefficient
        elif val < self.risky_thres:
            return ValueImportance.RISKY, self.coefficient
        else:
            return ValueImportance.DANGEROUS, self.coefficient
