import os
import uuid
import json
from catboost import CatBoostClassifier, Pool
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, average_precision_score
from sklearn.preprocessing import label_binarize

from cs_bitcoin_model.crypto.aggregate.btc_prediction.binance_client import BinanceClient
import pandas as pd
from sklearn.model_selection import train_test_split

from cs_bitcoin_model.crypto.utils.cs_config import CSConfig


def get_history_price(coin_pair: str):
    client = BinanceClient()
    df = None
    if client.get_support_coin_pair(coin_pair):
        df = client.get_historical_data(2000, coin_pair)

    df = df.reindex(index=df.index[::-1])
    client.close()

    return df


def convert_str_to_date(col):
    date_str = col["date"]
    if date_str[-1:] == "M":
        if date_str[-2:] == "AM":
            date_str = date_str.replace("-AM", ":00:00")
        else:
            convert_time = str(int(date_str[-5:-3]) + 12)
            convert_time = convert_time if convert_time != "24" else "00"
            convert_time = convert_time + ":00:00"
            date_str = date_str.replace(date_str[-5:], convert_time)
    col["date"] = date_str
    return col


def pre_preprocess_3_labels(df):
    df = df.reset_index().rename(columns={"dateTime": "date"})
    df[["open", "high", "low", "close", "tradecount", "volume_btc", "volume_usdt"]] = \
        df[["open", "high", "low", "close", "tradecount", "volume_btc", "volume_usdt"]].apply(pd.to_numeric)
    df = df.apply(convert_str_to_date, axis=1)
    df["tradecount"] = df["tradecount"].fillna(0)
    df['date'] = pd.to_datetime(df['date'], format="%Y_%m_%d %H:%M:%S")
    df = df.sort_values(by="date", ascending=False)

    df = df.assign(next_close=df.close.shift(1))
    df.drop(index=0, inplace=True)
    df.reset_index(drop=True, inplace=True)

    df["label"] = (df["next_close"] - df["close"]) / df["close"] * 100
    #     df["label"] = df["label"].astype(int)

    df.loc[df["label"] < -0.5, "label"] = 0
    df.loc[df["label"] > 0.5, "label"] = 2
    df.loc[(df["label"] != 0) & (df["label"] != 2), "label"] = 1

    return df


def train_model(X_train, y_train, eval_set=None, grid_search=False, weights=None, model_params={}):
    if grid_search:
        search_params = {
            'learning_rate': [0.03],
            'depth': [9, 11, 13],
            'iterations': [2000, 5000],
        }

        clf = CatBoostClassifier(task_type="GPU", devices='0:1', eval_metric='AUC', )
        grid_result = clf.grid_search(search_params, X=X_train, y=y_train, plot=True, cv=5)
        print(f"Best parameter: {grid_result['params']}")
    else:
        pool = Pool(data=X_train, label=y_train, weight=weights)
        clf = CatBoostClassifier(**model_params)
        clf.fit(pool, eval_set=eval_set, plot=True, verbose=500)

    return clf


def train_and_evaluate(train_df_view, test_df_view, params: map, valid_df_view=None, train_label_col='label'):
    weights = None
    features = params.get('features', train_df_view.columns.values)
    model_params = {
        'learning_rate': 0.04,
        'iterations': 10000,
        'depth': 5,
        'l2_leaf_reg': 4,
        #     'task_type': 'GPU',
        #     'devices': '0',
        #     'loss_function': 'CrossEntropy',
        # 'eval_metric': 'AUC',
        'early_stopping_rounds': 2000,
        'use_best_model': True
    }

    # train
    eval_set = None
    if valid_df_view is not None:
        eval_set = Pool(valid_df_view[features], valid_df_view['label'])

    clf = train_model(
        train_df_view[features],
        train_df_view[train_label_col],
        eval_set=eval_set,
        grid_search=False,
        weights=weights,
        model_params=model_params
    )

    return clf


def benchmark(model, df_test, y, leaderboard, run_name, push=False, save=False):
    features = model.feature_names_
    X = df_test[features]
    Y = label_binarize(y, classes=[0, 1, 2])

    y_predict_proba = model.predict_proba(X)
    y_predict = model.predict(X)

    accuracy = accuracy_score(y, y_predict)
    precision_aver = precision_score(y, y_predict, average='weighted')
    recall_aver = recall_score(y, y_predict, average='weighted')
    f1_aver = f1_score(y, y_predict, average='weighted')

    # For each class
    average_precision_class = dict()
    for i in range(3):
        average_precision_class[i] = average_precision_score(Y[:, i], y_predict_proba[:, i])

    metrics = {
        'Accuracy': accuracy,
        'Precision': precision_aver,
        'Recall': recall_aver,
        'F1': f1_aver,
        'Precision_class_0': average_precision_class[0],
        'Precision_class_1': average_precision_class[1],
        'Precision_class_2': average_precision_class[2]
    }

    # save model
    if save:
        name = f'{run_name}_{str(uuid.uuid4())}'
        path = f'/home/hungph/model/btc_cb/{name}'
        save_model(model, df_test, path, name=name, accuracy=metrics["Accuracy"])
        artifacts = [path]

    if push:
        params = dict(model.get_all_params())
        # push_mlflow(leaderboard, run_name, metrics, params, artifacts, model)

    return metrics


def save_model(clf, df, path, name, accuracy):
    os.makedirs(path, exist_ok=True)
    clf.save_model(path + "/parameter.dat")

    features = []
    for feature in clf.feature_names_:
        desc = desc_feature(df, feature, [])
        features.append(desc)

    desc = json.dumps({
        'type': 'CATBOOST_BINOMIAL',
        'name': name,
        'features': features,
        'accuracy': accuracy
    })

    with open(path + '/desc.json', 'w') as f:
        f.write(desc)


def desc_feature(train_df, feature, double_features):
    if feature not in double_features:
        values = train_df[feature].unique().tolist()
        min_v = min(values)
        max_v = max(values)

        return {'name': feature, 'type': 'INT', 'min': int(min_v), 'max': int(max_v)}
    else:
        values = train_df[feature].unique().tolist()
        min_v = min(values)
        max_v = max(values)

        return {'name': feature, 'type': 'DOUBLE', 'min': min_v, 'max': max_v}


def load_model(mdir):
    with open(f"{mdir}/desc.json") as f:
        desc = json.load(f)

    if desc['type'] == 'CATBOOST_BINOMIAL':
        clf = CatBoostClassifier()
        clf.load_model(f"{mdir}/parameter.dat")

    return desc, clf


def run():
    # Get all history data
    print("Getting all data from binance...")
    df = get_history_price("BTCUSDT")
    print(f"Getting data done with shape: {df.shape}")

    # Labeling to data with 3 labels as 0 - down, 1 - neutral, 2 - up
    df = pre_preprocess_3_labels(df)

    # Check balance of all labels
    print(df.label.value_counts())

    # Split train test
    df_test = df[:3000]
    df_train = df[3000:]
    df_train, df_valid = train_test_split(df_train, test_size=0.2)

    # train new model
    baseline_features = ["open", "high", "low", "close", "volume_btc", "volume_usdt", "tradecount"]
    params = {
        'features': baseline_features
    }

    clf_baseline = train_and_evaluate(df_train, df_test,
                                      valid_df_view=df_valid,
                                      params=params,
                                      train_label_col='label')

    # benchmark new model
    leaderboard = 'btc_up_down_2022_06_16'
    run_name = 'model_btc_classification_3_classes'
    new_metrics = benchmark(clf_baseline, df_test, df_test["label"], leaderboard, run_name, push=True)

    # benchmark old model
    parser = CSConfig("production", "BtcPredict", "config")
    current_model_path = parser.read_parameter("model_path")
    desc, old_model = load_model(current_model_path)
    old_metrics = benchmark(old_model, df_test, df_test["label"], leaderboard, run_name, push=False)

    # Compare old and new model
    print("Old metrics: ", old_metrics)
    print("New metrics: ", new_metrics)
    if new_metrics["F1"] > old_metrics["F1"]:
        df_all = pd.concat([df_train, df_test])
        clf_new_model = train_and_evaluate(df_all, df_valid,
                                          valid_df_view=df_valid,
                                          params=params,
                                          train_label_col='label')
        save_model(clf_new_model, df_all, current_model_path, name=run_name,accuracy=new_metrics["Accuracy"])


if __name__ == "__main__":
    run()
