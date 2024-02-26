from typing import Union

from fastapi import Depends, FastAPI, Security
from fastapi.security import OAuth2PasswordBearer, HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel
import uvicorn

app = FastAPI()

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")


class User(BaseModel):
    username: str
    email: Union[str, None] = None
    full_name: Union[str, None] = None
    disabled: Union[bool, None] = None


def fake_decode_token(token):
    return User(
        username=token + "fakedecoded", email="john@example.com", full_name="John Doe"
    )


async def get_current_user(token: str = Depends(oauth2_scheme)):
    user = fake_decode_token(token)
    return user


@app.get("/users/me")
async def read_users_me(current_user: User = Depends(get_current_user)):
    return current_user


def auth_wrapper(auth: HTTPAuthorizationCredentials = Security(HTTPBearer())):
    print("bearer: ",  auth.credentials)

    return "hungph"

@app.get("/protected")
def protected(username=Depends(auth_wrapper)):
    print("hungph")
    return {"name": username}


if __name__ == "__main__":
    # uvicorn.run(app=app,
    #             host="127.0.0.1",
    #             port=2096)
    # import json
    # # check_json = "{'user_id': '15', 'datetime': datetime.datetime(2022, 7, 10, 16, 5, 51), 'list_save_tweets': '1545893194851913729,1546083670599032833'}"
    # check_json = "{'user_id': '15', 'list_save_tweets': '1545893194851913729,1546083670599032833'}"
    # print(json.loads(json.dumps(check_json)))
    # print(type(json.loads(check_json.replace("'", "\""))))
    # print(check_json.replace("'", "\""))

    print("#BNB\\xa0".replace("\\x", " \\x"))
