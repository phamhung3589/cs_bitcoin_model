import datetime

import requests
import json

from datetime import date
from cs_bitcoin_model.crypto.aggregate.cryoto_quant.classify_indicator_ex_netflow import ClassifyIndicatorExNet
from cs_bitcoin_model.crypto.aggregate.cryoto_quant.classify_indicator_leverage_ratio import ClassifyIndicatorLeverageRatio
from cs_bitcoin_model.crypto.aggregate.cryoto_quant.classify_indicator_liquidation import ClassifyIndicatorLiquidation
from cs_bitcoin_model.crypto.aggregate.cryoto_quant.classify_indicator_mpi import ClassifyIndicatorMpi
from cs_bitcoin_model.crypto.aggregate.cryoto_quant.classify_indicator_mvrv import ClassifyIndicatorMvrv
from cs_bitcoin_model.crypto.aggregate.cryoto_quant.classify_indicator_nupl import ClassifyIndicatorNupl
from cs_bitcoin_model.crypto.aggregate.cryoto_quant.classify_indicator_nvt_golden import ClassifyIndicatorNvtGolden
from cs_bitcoin_model.crypto.aggregate.cryoto_quant.classify_indicator_puell import ClassifyIndicatorPuell
from cs_bitcoin_model.crypto.aggregate.cryoto_quant.classify_indicator_ratio_usd import ClassifyIndicatorRatioUsd
from cs_bitcoin_model.crypto.aggregate.cryoto_quant.classify_indicator_sopr import ClassifyIndicatorSopr
from cs_bitcoin_model.crypto.aggregate.cryoto_quant.classify_indicator_sopr_holders import ClassifyIndicatorSoprHolders
from cs_bitcoin_model.crypto.aggregate.cryoto_quant.classify_indicator_utxo import ClassifyIndicatorUtxo
from cs_bitcoin_model.crypto.utils import constants
from cs_bitcoin_model.crypto.utils.cs_config import CSConfig
from cs_bitcoin_model.crypto.utils.db_utils import get_output


class AggregateCryptoQuanIndicator:
    """
    - Class processing logic involving indicator getting from cryptoquant api
    """
    def __init__(self, raw_value=False):
        parser = CSConfig("production", self.__class__.__name__, "config")
        self.url_mpi = parser.read_parameter("url_mpi")
        self.url_ex_net = parser.read_parameter("url_exchange_netflow")
        self.url_ratio_usd = parser.read_parameter("url_stablecoins_ratio_usd")
        self.url_leverage_ratio = parser.read_parameter("url_leverage_ratio")
        self.url_sopr = parser.read_parameter("url_sopr")
        self.url_sopr_holders = parser.read_parameter(
            "url_sopr_longterm_holders")
        self.url_mvrv = parser.read_parameter("url_mvrv")
        self.url_nupl = parser.read_parameter("url_nupl")
        self.url_nvt_golden_cross = parser.read_parameter(
            "url_nvt_golden_cross")
        self.url_puell_multiple = parser.read_parameter("url_puell_multiple")
        self.url_utxo = parser.read_parameter("url_utxo")
        self.url_liquidation = parser.read_parameter("url_liquidation")
        key = CSConfig("production", self.__class__.__name__, "key").read_parameter("key")
        self.headers = {'Authorization': 'Bearer ' + key}
        self.result = {}
        self.raw_value = raw_value
        super()

    def create_result(self, running_date):
        """
        :return: result with all name = ""
        """
        ind_result = dict()

        for ind in constants.list_indicators:
            ind_result[ind] = ""

        ind_result["datetime"] = datetime.datetime.strptime(running_date, "%Y%m%d").date()

        return ind_result

    def get_indicator_mpi(self, running_date):
        """
        :return: Processing mpi indicator
        """
        self.url_mpi = get_output(self.url_mpi, running_date, running_date)
        response = requests.get(self.url_mpi, headers=self.headers).json()

        if response["status"]["code"] == 200:
            data = response["result"]["data"][0]
            indicator_val = data["mpi"]

            if self.raw_value:
                return indicator_val

            indicator_type, indicator_coeff = ClassifyIndicatorMpi().check_indicator(indicator_val)
            return indicator_type + "-" + indicator_coeff

        return None

    def get_indicator_exchange_netflow(self, running_date):
        """
        :return: Processing exchange_netflow indicator
        """
        self.url_ex_net = get_output(self.url_ex_net, running_date, running_date)
        response = requests.get(self.url_ex_net, headers=self.headers).json()

        if response["status"]["code"] == 200:
            data = response["result"]["data"][0]
            indicator_val = data["netflow_total"]

            if self.raw_value:
                return indicator_val

            indicator_type, indicator_coeff = ClassifyIndicatorExNet().check_indicator(indicator_val)
            return indicator_type + "-" + indicator_coeff

        return None

    def get_indicator_stablecoins_ratio_usd(self, running_date):
        """
        :param running_date: date that need to get indicator
        :return: Processing stablecoins_ratio_usd indicator
        """
        self.url_ratio_usd = get_output(self.url_ratio_usd, running_date, running_date)
        response = requests.get(self.url_ratio_usd, headers=self.headers).json()

        if response["status"]["code"] == 200:
            data = response["result"]["data"][0]
            stablecoins_ratio = data["stablecoins_ratio_usd"]

            if self.raw_value:
                return stablecoins_ratio

            ratio_type, ratio_coeff = ClassifyIndicatorRatioUsd().check_indicator(stablecoins_ratio)
            return ratio_type + "-" + ratio_coeff

        return None

    def get_indicator_leverage_ratio(self, running_date):
        """
        :return: Processing leverage_ratio indicator
        """
        self.url_leverage_ratio = get_output(self.url_leverage_ratio, running_date, running_date)
        response = requests.get(self.url_leverage_ratio, headers=self.headers).json()

        if response["status"]["code"] == 200:
            data = response["result"]["data"][0]
            indicator_val = data["estimated_leverage_ratio"]

            if self.raw_value:
                return indicator_val

            indicator_type, indicator_coeff = ClassifyIndicatorLeverageRatio().check_indicator(indicator_val)
            return indicator_type + "-" + indicator_coeff

        return None

    def get_indicator_sopr(self, running_date):
        """
        :return: Processing sopr indicator
        """
        self.url_sopr = get_output(self.url_sopr, running_date, running_date)
        response = requests.get(self.url_sopr, headers=self.headers).json()

        if response["status"]["code"] == 200:
            data = response["result"]["data"][0]
            indicator_val = data["sopr"]

            if self.raw_value:
                return indicator_val

            indicator_type, indicator_coeff = ClassifyIndicatorSopr().check_indicator(indicator_val)
            return indicator_type + "-" + indicator_coeff

        return None

    def get_indicator_sopr_longterm_holders(self, running_date):
        """
        :return: Processing sopr_longterm_holders indicator
        """
        self.url_sopr_holders = get_output(self.url_sopr_holders, running_date, running_date)
        response = requests.get(self.url_sopr_holders, headers=self.headers).json()

        if response["status"]["code"] == 200:
            data = response["result"]["data"][0]
            indicator_val = data["sopr_ratio"]

            if self.raw_value:
                return indicator_val

            indicator_type, indicator_coeff = ClassifyIndicatorSoprHolders().check_indicator(indicator_val)
            return indicator_type + "-" + indicator_coeff

        return None

    def get_indicator_mvrv(self, running_date):
        """
        :return: Processing mvrv indicator
        """
        self.url_mvrv = get_output(self.url_mvrv, running_date, running_date)
        response = requests.get(self.url_mvrv, headers=self.headers).json()

        if response["status"]["code"] == 200:
            data = response["result"]["data"][0]
            indicator_val = data["mvrv"]

            if self.raw_value:
                return indicator_val

            indicator_type, indicator_coeff = ClassifyIndicatorMvrv().check_indicator(indicator_val)
            return indicator_type + "-" + indicator_coeff

        return None

    def get_indicator_nupl(self, running_date):
        """
        :return: Processing nupl indicator
        """
        self.url_nupl = get_output(self.url_nupl, running_date, running_date)
        response = requests.get(self.url_nupl, headers=self.headers).json()

        if response["status"]["code"] == 200:
            data = response["result"]["data"][0]
            indicator_val = data["nupl"]

            if self.raw_value:
                return indicator_val

            indicator_type, indicator_coeff = ClassifyIndicatorNupl().check_indicator(indicator_val)
            return indicator_type + "-" + indicator_coeff

        return None

    def get_indicator_nvt_golden_cross(self, running_date):
        """
        :return: Processing nvt_golden_cross indicator
        """
        self.url_nvt_golden_cross = get_output(self.url_nvt_golden_cross, running_date, running_date)
        response = requests.get(self.url_nvt_golden_cross, headers=self.headers).json()

        if response["status"]["code"] == 200:
            data = response["result"]["data"][0]
            indicator_val = data["nvt_golden_cross"]

            if self.raw_value:
                return indicator_val

            indicator_type, indicator_coeff = ClassifyIndicatorNvtGolden().check_indicator(indicator_val)
            return indicator_type + "-" + indicator_coeff

        return None

    def get_indicator_puell_multiple(self, running_date):
        """
        :return: Processing puell_multiple indicator
        """
        self.url_puell_multiple = get_output(self.url_puell_multiple, running_date, running_date)
        response = requests.get(self.url_puell_multiple, headers=self.headers).json()

        if response["status"]["code"] == 200:
            data = response["result"]["data"][0]
            indicator_val = data["puell_multiple"]

            if self.raw_value:
                return indicator_val

            indicator_type, indicator_coeff = ClassifyIndicatorPuell().check_indicator(indicator_val)
            return indicator_type + "-" + indicator_coeff

        return None

    def get_indicator_utxo(self, running_date):
        """
        :return: Processing utxo indicator
        """
        self.url_utxo = get_output(self.url_utxo, running_date, running_date)
        response = requests.get(self.url_utxo, headers=self.headers).json()

        if response["status"]["code"] == 200:
            data = response["result"]["data"][0]
            indicator_val = data["profit_percent"]

            if self.raw_value:
                return indicator_val

            indicator_type, indicator_coeff = ClassifyIndicatorUtxo().check_indicator(indicator_val)
            return indicator_type + "-" + indicator_coeff

        return None

    def get_indicator_liquidation(self, running_date):
        """
        :return: Processing liquidation indicator
        """
        self.url_liquidation = get_output(self.url_liquidation, running_date, running_date)
        response = requests.get(self.url_liquidation, headers=self.headers).json()

        if response["status"]["code"] == 200:
            data = response["result"]["data"][0]
            indicator_long_val = data["long_liquidations"]
            indicator_short_val = data["short_liquidations"]

            if self.raw_value:
                return indicator_long_val, indicator_short_val

            indicator_result = ClassifyIndicatorLiquidation().check_indicator(indicator_long_val, indicator_short_val)
            return (indicator_result["long_liq_type"] + "-" + indicator_result["long_coef"],
                    indicator_result["short_liq_type"] + "-" + indicator_result["short_coef"])

        return None

    def aggregate_indicator(self, running_date):
        """
        :return: aggregate all indicators in to dict
        """
        self.result = self.create_result(running_date)
        ind_mpi = self.get_indicator_mpi(running_date)
        ind_ex_net = self.get_indicator_exchange_netflow(running_date)
        ind_ratio_usd = self.get_indicator_stablecoins_ratio_usd(running_date)
        ind_leverage_ratio = self.get_indicator_leverage_ratio(running_date)
        ind_sopr = self.get_indicator_sopr(running_date)
        ind_sopr_holders = self.get_indicator_sopr_longterm_holders(running_date)
        ind_mvrv = self.get_indicator_mvrv(running_date)
        ind_nupl = self.get_indicator_nupl(running_date)
        ind_nvt_golden_cross = self.get_indicator_nvt_golden_cross(running_date)
        ind_puell_multiple = self.get_indicator_puell_multiple(running_date)
        ind_utxo = self.get_indicator_utxo(running_date)
        ind_liquidation = self.get_indicator_liquidation(running_date)

        if ind_mpi is not None:
            self.result[constants.ind_mpi] = ind_mpi

        if ind_ex_net is not None:
            self.result[constants.ind_ex_net] = ind_ex_net

        if ind_ratio_usd is not None:
            self.result[constants.ind_ratio_usd] = ind_ratio_usd

        if ind_leverage_ratio is not None:
            self.result[constants.ind_leverage_ratio] = ind_leverage_ratio

        if ind_sopr is not None:
            self.result[constants.ind_sopr] = ind_sopr

        if ind_sopr_holders is not None:
            self.result[constants.ind_sopr_holders] = ind_sopr_holders

        if ind_mvrv is not None:
            self.result[constants.ind_mvrv] = ind_mvrv

        if ind_nupl is not None:
            self.result[constants.ind_nupl] = ind_nupl

        if ind_nvt_golden_cross is not None:
            self.result[constants.ind_nvt_golden_cross] = ind_nvt_golden_cross

        if ind_puell_multiple is not None:
            self.result[constants.ind_puell] = ind_puell_multiple

        if ind_utxo is not None:
            self.result[constants.ind_utxo] = ind_utxo

        if ind_liquidation is not None:
            self.result[constants.ind_long_liquidation], self.result[constants.ind_short_liquidation] = ind_liquidation

        return self.result


if __name__ == "__main__":
    quant_api = AggregateCryptoQuanIndicator(True)
    result = quant_api.aggregate_indicator("20220304")
    print(result)
