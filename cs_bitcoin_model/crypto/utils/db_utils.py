from jose import JWTError, jwt
import base64

from cs_bitcoin_model.crypto.utils.constants import Constants


def parse_string(data: str):
    return data.split("-")


def get_output(url: str, from_date: str, to_date: str):
    return url.replace("{from_date}", from_date).replace("{to_date}", to_date)


def decode_token(token_id):
    payloads = jwt.decode(token=token_id, key=base64.b64decode(Constants.JWT_SECRET), algorithms=[Constants.JWT_ALGORITHM])

    return payloads["sub"]


def calculate_sloth_index(type, coeff):
    if coeff == "M":
        int_coeff = 1
    elif coeff == "H":
        int_coeff = 1.5
    else:
        int_coeff = 2

    if type == "EXCELLENT":
        int_type = 5
    elif type == "GOOD":
        int_type = 4
    elif type == "AVERAGE":
        int_type = 3
    elif type == "RISKY":
        int_type = 2
    else:
        int_type = 1

    return int_type * int_coeff
