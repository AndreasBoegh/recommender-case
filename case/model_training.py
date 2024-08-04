import pandas as pd
from lifetimes import BetaGeoFitter
from lifetimes import GammaGammaFitter


def main():
    df = pd.read_parquet("./data/preprocessed_data")

    bgf = BetaGeoFitter(penalizer_coef=0.001)  # model object
    bgf.fit(df["Frequency"], df["Recency"], df["T"])  # model fitting

    df["expctd_num_of_purch"] = bgf.predict(
        365, df["Frequency"], df["Recency"], df["T"]
    )

    ggf = GammaGammaFitter(penalizer_coef=0.01)  # model object
    ggf.fit(df["Frequency"], df["Monetary"])  # model fitting

    df["expct_avg_spend"] = ggf.conditional_expected_average_profit(
        df["Frequency"], df["Monetary"]
    )

    df["cltv_one_year"] = ggf.customer_lifetime_value(
        bgf,
        df["Frequency"],
        df["Recency"],
        df["T"],
        df["Monetary"],
        time=12,  # 12 month
        freq="D",  # frequency of T
        discount_rate=0.01,
    )

    df.to_json("./data/model_output.json", orient="records")
