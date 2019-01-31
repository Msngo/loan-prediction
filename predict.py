import os
import settings
import pandas as pd
from sklearn.model_selection import cross_val_predict
from sklearn.linear_model import LogisticRegression
from sklearn import metrics


def cross_validate(train):
    """ (DataFrame) -> ndarray

    Run cross validation
    """
    clf = LogisticRegression(class_weight="balanced", solver='lbfgs' random_state=1)

    predictors = train.columns.tolist()
    predictors = [p for p in predictors if p not in settings.NON_PREDICTORS]

    predictions = cross_val_predict(
        clf, train[predictors], train[settings.TARGET], cv=settings.CV_FOLDS)

    return predictions


def compute_error(target, predictions):
    """ ()
    """
    return metrics.accuracy_score(target, predictions)


def compute_false_negatives(target, predictions):
    df = pd.DataFrame({"target": target, "predictions": predictions})
    return df[(df['target'] == 1) & (df['predictions'] == 0)].shape[0] / (df[
        (df['target'] == 1)].shape[0] + 1)


def compute_false_positives(target, predictions):
    df = pd.DataFrame({"target": target, "predictions": predictions})
    return df[(df["target"] == 0) & (df["predictions"] == 1)].shape[0] / (df[
        (df["target"] == 0)].shape[0] + 1)


def read():
    train = pd.read_csv(os.path.join(settings.PROCESSED_DIR, "train.csv"))
    return train


if __name__ == '__main__':
    print("Beginning of predict.py file...")
    train = read()
    predictions = cross_validate(train)
    error = compute_error(train[settings.TARGET], predictions)
    fn = compute_false_negatives(train[settings.TARGET], predictions)
    fp = compute_false_positives(train[settings.TARGET], predictions)
    print("Accuracy Score: {}".format(error))
    print("False Negatives: {}".format(fn))
    print("False Positives: {}".format(fp))
    print("End of predict.py file...")
