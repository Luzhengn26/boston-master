import time
import warnings

warnings.filterwarnings("ignore")
from datetime import datetime, timedelta
from os import path

from utils.datalib_database import df_from_sql

import math
import pandas as pd
import numpy as np
from scipy import stats
from scipy.stats import chi2

import matplotlib.pyplot as plt

plt.rcParams["figure.figsize"] = (16, 10)
import seaborn as sns

sns.set()

pd.set_option("display.float_format", lambda x: "%.3f" % x)

import statsmodels.api as sm
import operator

####################################################################
# Functions to get data from DWH


def timer(elapsed_time):
    return time.strftime("%H:%M:%S", time.gmtime(elapsed_time))


def generate_filename(folder, ticket_number, description, date_start, date_end):
    return "{}{}-extract-{}-{}-{}.csv".format(
        folder,
        ticket_number,
        description,
        datetime.strftime(date_start, "%Y%m%d"),
        datetime.strftime(date_end, "%Y%m%d"),
    )


def query_data(query):
    return df_from_sql("redshiftreader", query)


def save_data(data, filename):
    data.to_csv(filename, index=False)


def get_data(query, filename):
    ts = time.time()
    data = query_data(query)
    te = time.time() - ts
    print("Finished querying data. Time elapsed: {}.".format(timer(te)))

    ts = time.time()
    save_data(data, filename)
    te = time.time() - ts
    print("Finished saving data. Time elapsed: {}.\n".format(timer(te)))


def check_file_exist(filename):
    return path.exists(filename)


####################################################################
# Correct dtypes in DataFrame


def cols_as_object(data, cols):
    for col in cols:
        data[col] = data[col].astype("object")


def cols_as_int(data, cols):
    for col in cols:
        data[col] = data[col].astype("int")


def cols_as_datetime(data, cols):
    for col in cols:
        data[col] = pd.to_datetime(data[col]).dt.tz_localize(None)


def correct_dtypes(data, cols_obj, cols_int, cols_dt):
    cols_as_object(data, cols_obj)
    cols_as_int(data, cols_int)
    cols_as_datetime(data, cols_dt)


####################################################################
# Get list of different types of columns from a DataFrame


def get_cat_cols(data):
    return data.select_dtypes(include=["category"]).columns


def get_obj_cols(data):
    return data.select_dtypes(include=["object"]).columns


def get_str_cols(data):
    return data.select_dtypes(include=["category", "object", "bool"]).columns


def get_num_cols(data):
    return data.select_dtypes(include=["int", "float"]).columns


####################################################################
# Analysis and modelling


def get_crosstab_plots(data, pred, feats):
    nrows = math.ceil(len(feats) / 2)
    fig, axes = plt.subplots(
        figsize=(16, nrows * 6), nrows=nrows, ncols=2, sharex=True, sharey=False
    )

    for i, ax in enumerate(axes.flatten()):
        if i < len(feats):
            feat = feats[i]
            xtab_abs = pd.crosstab(data[feat], data[pred])
            xtab_perc = xtab_abs.div(xtab_abs.sum(1).astype(float), axis=0)
            xtab_perc.plot(kind="barh", stacked=True, ax=ax)
            ax.set_title("{}".format(feat))
            ax.set_xlabel("Proportion of {}".format(pred))
            ax.set_ylabel("")

    plt.tight_layout()
    plt.show()


def get_boxplots(data, pred, feats):
    nrows = math.ceil(len(feats) / 2)
    fig, axes = plt.subplots(
        figsize=(16, nrows * 6), nrows=nrows, ncols=2, sharex=True, sharey=False
    )

    for i, ax in enumerate(axes.flatten()):
        if i < len(feats):
            feat = feats[i]
            sns.boxplot(x=pred, y=feat, data=data, whis=[0, 100], width=0.6, ax=axes[i])
            ax.xaxis.grid(True)
            ax.set_title("Boxplot of {} vs. {}".format(pred, feat))
            ax.set_xlabel("{}".format(pred))
            ax.set_ylabel("{}".format(feat))

    # sns.despine(trim=True, left=True)
    plt.tight_layout()
    plt.show()


def two_sample_ttest(a, b, tail):
    # calculate two-tailed test
    t_stat, p_val = stats.ttest_ind(a, b)

    if tail == "left":
        p_val = stats.t.cdf(t_stat, len(a) + len(b) - 2)
    elif tail == "right":
        p_val = stats.t.sf(t_stat, len(a) + len(b) - 2)
    print("t stat: \t{:.4f}\np value: \t{:4f}".format(t_stat, p_val))
    return t_stat, p_val


def chi_square_test(contingency_table):
    chisquare, p_val, dof, exp_val = stats.chi2_contingency(contingency_table)

    # find critical values
    significance = 0.01
    p = 1 - significance
    critical_value = chi2.ppf(p, dof)

    # reject with chi-square
    print("chi=%.6f, critical value=%.6f\n" % (chisquare, critical_value))
    if chisquare > critical_value:
        print(
            """At %.2f level of significance, we reject the null hypotheses and accept H1. 
    They are not independent."""
            % (significance)
        )
    else:
        print(
            """At %.2f level of significance, we fail to reject the null hypotheses. 
    They are independent."""
            % (significance)
        )

    # reject with p-value
    print("\np-value=%.6f, significance=%.2f\n" % (p_val, significance))
    if p_val < significance:
        print(
            """At %.2f level of significance, we reject the null hypotheses and accept H1. 
    They are not independent."""
            % (significance)
        )
    else:
        print(
            """At %.2f level of significance, we fail to reject the null hypotheses. 
    They are independent."""
            % (significance)
        )

    return chisquare, p_val, dof, exp_val, critical_value


def remove_most_insignificant(df, result):
    # use operator to find the key which belongs to the maximum value in the dictionary:
    max_p_value = max(result.pvalues.iteritems(), key=operator.itemgetter(1))[0]
    # this is the feature you want to drop:
    df.drop(columns=max_p_value, inplace=True)
    return df


def run_logit(data, select_feats, pred, drop_dummies, significance=False):
    if significance == False:
        X = data[select_feats]
        y = data[pred]
        cat_cols = get_str_cols(X)
        X = pd.get_dummies(X, columns=cat_cols, drop_first=False)
        for drop_dum in drop_dummies:
            try:
                X.drop(drop_dum, axis=1, inplace=True)
            except:
                continue
        X["intercept"] = 1

        print("Running Logit with {} feature(s).".format(len(select_feats)))

        log_mod = sm.Logit(y, X.astype(float))
        result = log_mod.fit(disp=0)

    elif significance == True:
        X = data[select_feats]
        y = data[pred]
        cat_cols = get_str_cols(X)
        X = pd.get_dummies(X, columns=cat_cols, drop_first=False)
        for drop_dum in drop_dummies:
            try:
                X.drop(drop_dum, axis=1, inplace=True)
            except:
                continue
        X["intercept"] = 1

        insignificant_feature = True
        while insignificant_feature:
            model = sm.Logit(y, X.astype(float))
            result = model.fit()
            significant = [p_value < 0.05 for p_value in result.pvalues]
            if all(significant):
                insignificant_feature = False
            else:
                if X.shape[1] == 1:  # if there's only one insignificant variable left
                    print("No significant features found")
                    result = None
                    insignificant_feature = False
                else:
                    X = remove_most_insignificant(X, result)

    params = result.params
    conf = result.conf_int()
    conf["Odds Ratio"] = params
    conf.columns = ["5%", "95%", "Odds Ratio"]
    odds_ratios = np.exp(conf)

    AME = result.get_margeff()

    return result, odds_ratios, AME
