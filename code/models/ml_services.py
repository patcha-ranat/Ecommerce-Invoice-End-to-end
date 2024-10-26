from datetime import datetime
import logging

import pandas as pd
import numpy as np
from sklearn.preprocessing import RobustScaler
from sklearn.cluster import KMeans
from sklearn.model_selection import RandomizedSearchCV, train_test_split
from lightgbm import LGBMClassifier
from sklearn.metrics import f1_score, accuracy_score, precision_score, recall_score
from sklearn.inspection import permutation_importance

from abstract import AbstractMLService, AbstractMLProcessor


# set pandas warning
pd.options.mode.chained_assignment = None  # default='warn'

class BaseMLService(AbstractMLService):
    def __init__(self):
        super().__init__()

    @property
    def __str__(self):
        # TODO enrich logging to ml service
        bases = [base.__name__ for base in self.__class__.__bases__]
        bases.append(self.__class__.__name__)
        return ".".join(bases)


class CustomerProfilingService(BaseMLService):
    """
    A Service to apply logic into e-commerce sales transaction to formulate customer profile with RFM framework.

    Methods
    -------
    - `process`: main process to orchestrate all the logic
        - `drop_anonymous`
        - `distinct_customer_invoice`
        - `merge_rfm`
            - `get_recency`
            - `get_frequency`
            - `get_monetary`
        - `feature_en_additional`

    Example
    -------
    ```
    instance = CustomerProfilingService()
    instance.get_input(df=df)
    enriched_customer_profile = instance.process()
    ```
    """

    def __init__(self, df: pd.DataFrame):
        super().__init__()
        self.df = df
        self.unique_invoice: pd.DataFrame = None
        self.customer_profile: pd.DataFrame

    def drop_anonymous(self, df: pd.DataFrame) -> pd.DataFrame:
        """Drop CustomerID = '0' from sample DataFrame"""
        anonymous_customer_index = df[df["CustomerID"] == 0].index
        df = df.drop(index=anonymous_customer_index)
        return df

    def distinct_customer_invoice(self, df: pd.DataFrame = None) -> pd.DataFrame:
        """Create/update class attribute of unique_invoice with unique customer-invoice DataFrame for re-usage"""
        if self.unique_invoice is not None:
            return self.unique_invoice.copy()
        else:
            unique_invoice = df[
                ["CustomerID", "InvoiceNo", "InvoiceDate"]
            ].drop_duplicates(["InvoiceNo"])
            self.unique_invoice = unique_invoice
            return self.unique_invoice.copy()

    def get_recency(self, df: pd.DataFrame | None = None) -> pd.DataFrame:
        """Create Recency DataFrame for RFM framework"""
        unique_invoice = self.distinct_customer_invoice(df=df)

        unique_invoice["recency"] = (
            unique_invoice.groupby("CustomerID")["InvoiceDate"].diff().dt.days
        )
        recency_df = unique_invoice.drop_duplicates("CustomerID", keep="last")

        # null value occured in recency for first time buyer
        calculation_date = unique_invoice["InvoiceDate"].max()
        recency_df["recency"] = (
            recency_df["recency"]
            .fillna((calculation_date - unique_invoice["InvoiceDate"]))
            .apply(lambda x: x if type(x) == float else x.days)
            .astype(int)
        )
        recency_df = recency_df.drop(columns=["InvoiceNo", "InvoiceDate"])

        return recency_df

    def get_frequency(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create Frequency DataFrame for RFM framework"""
        freq_df = (
            df.groupby(["CustomerID"])["InvoiceNo"]
            .nunique()
            .reset_index()
            .rename({"InvoiceNo": "frequency"}, axis=1)
        )
        return freq_df

    def get_monetary(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create Monetary DataFrame for RFM framework"""
        monetary_df = (
            df.groupby("CustomerID")["total_spend"]
            .sum()
            .reset_index()
            .rename({"total_spend": "monetary"}, axis=1)
        )
        return monetary_df

    def merge_rfm(self, rfm_dfs: list[pd.DataFrame]) -> pd.DataFrame:
        """Merge all rfm properties into a DataFrame as customer_profile DataFrame"""
        customer_profile = (
            rfm_dfs[0]
            .merge(rfm_dfs[1], on="CustomerID")
            .merge(rfm_dfs[2], on="CustomerID")
        )
        self.customer_profile = customer_profile
        return customer_profile

    def feature_en_additional(
        self, 
        customer_profile: pd.DataFrame, 
        df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Execute Feature Engineering for a better customer behavior segmentation
        1. is_first_time_buyer
        2. mean time between purchases
        3. mean ticket_size (AVG spent per transaction) + mean_qty + mean_unique_item
        4. mean spent per month + freq per month

        And then post-processing with merging to customer_profile (input RFM) returning a DataFrame
        as 'enriched_customer_profile'
        """
        # 1. is_first_time_buyer
        customer_profile.loc[
            customer_profile["recency"].isnull(), "is_first_time_buyer"
        ] = int(1)
        customer_profile["is_first_time_buyer"] = (
            customer_profile["is_first_time_buyer"].fillna(0).astype(int)
        )

        # 2. mean time between purchases
        # prep recency
        unique_invoice: pd.DataFrame = self.distinct_customer_invoice()
        unique_invoice["recency"] = (
            unique_invoice.groupby("CustomerID")["InvoiceDate"].diff().dt.days
        )
        # calculate mean recency
        mean_time_interval = (
            unique_invoice.groupby("CustomerID")
            .agg({"recency": lambda x: x.diff().abs().mean()})
            .reset_index()
            .sort_values("CustomerID")
        )
        mean_time_interval = mean_time_interval.rename(
            columns={"recency": "mean_time_interval"}
        )
        mean_time_interval["mean_time_interval"] = mean_time_interval[
            "mean_time_interval"
        ].apply(lambda x: round(x, 2))
        # fill null for first time customer with calculated exact recency
        recency_customer_profile = customer_profile.sort_values(
            "CustomerID"
        ).reset_index(drop=True)
        mean_time_interval = mean_time_interval.combine_first(
            recency_customer_profile.sort_values("CustomerID")
            .rename(columns={"recency": "mean_time_interval"})
            .reset_index(drop=True)
        )

        # 3. mean ticket_size (AVG spent per transaction) + mean_qty + mean_unique_item
        mean_per_purchase = (
            df.groupby(["CustomerID", "InvoiceNo"])
            .agg(
                {
                    "total_spend": "sum",  # aggregate some for each InvoiceNo and CustomerID
                    "Quantity": "sum",
                    "StockCode": "nunique",
                }
            )
            .groupby("CustomerID")
            .agg(
                {
                    "total_spend": "mean",  # aggregate mean for each CustomerID
                    "Quantity": "mean",
                    "StockCode": "mean",
                }
            )
            .reset_index()
            .rename(
                columns={
                    "total_spend": "mean_ticket_size",
                    "Quantity": "mean_quantity",
                    "StockCode": "mean_unique_item",
                }
            )
            .round(2)
        )

        # 4. mean spent per month + freq per month
        per_period = df[
            ["CustomerID", "InvoiceNo", "StockCode", "InvoiceDate", "total_spend"]
        ].copy()
        per_period["month"] = per_period["InvoiceDate"].dt.month

        per_month = (
            per_period.groupby(["CustomerID", "month"])
            .agg({"InvoiceNo": "nunique", "total_spend": "mean"})
            .groupby(["CustomerID"])
            .agg({"InvoiceNo": "mean", "total_spend": "mean"})
            .round(2)
            .reset_index()
            .rename(
                columns={
                    "InvoiceNo": "freq_per_month",
                    "total_spend": "mean_spent_per_month",
                }
            )
        )

        # post-process: aggregate engineered features to customer_profile
        enriched_customer_profile = (
            customer_profile.merge(mean_time_interval, on="CustomerID")
            .merge(mean_per_purchase, on="CustomerID")
            .merge(per_month, on="CustomerID")
        )
        return enriched_customer_profile

    def process(self) -> pd.DataFrame:
        """Entrypoint for data preprocessing"""
        df = self.drop_anonymous(df=self.df)
        recency_df = self.get_recency(df=df)
        frequency_df = self.get_frequency(df=df)
        monetary_df = self.get_monetary(df=df)
        customer_profile = self.merge_rfm(
            rfm_dfs=[recency_df, frequency_df, monetary_df]
        )
        enriched_customer_profile = self.feature_en_additional(
            customer_profile=customer_profile, df=df
        )
        return enriched_customer_profile


class CustomerSegmentationService(BaseMLService):
    """
    Segment customer into clusters using their buying behavior (customer profile) and KMeans algorithm

    Methods
    -------
    - `process`: main process to orchestrate clustering processes
        - `scale`: scale dataset for KMeans algorithm handling euclidean distance problems
        - `train`: create one-time used distortions list for further process finding an optimal K value.
        - `find_best_elbow`: dynamically find optimal K value considering derivative of distortions sudo-function.
        - `clustering`: retrain with the optimal K value and assign cluster to the DataFrame

    Example
    -------
    ```
    instance = CustomerSegmentationService()
    instance.get_input(df=enriched_customer_profile)
    output: dict = instance.process()
    ```
    """

    def __init__(self, df: pd.DataFrame):
        super().__init__()
        self.df = df
        self.scaler: RobustScaler

    def scale(self, df: pd.DataFrame) -> tuple[pd.DataFrame, RobustScaler]:
        """
        Scale DataFrame with RobustScaler\n
        Input: enriched_customer_profile (rfm + engineered features)\n
        Output: scaled enriched_customer_profile for being KMeans input

        :param df: enriched_customer_profile (or customer_profile/rfm)
        :type df: pd.DataFrame
        """
        scaler = RobustScaler()
        scaled_df = scaler.fit_transform(df.drop(columns=["CustomerID"]))

        # save output
        self.scaler = scaler
        scaled_df = pd.DataFrame(
            scaled_df, columns=df.drop(columns=["CustomerID"]).columns
        )

        return scaled_df, scaler

    def train(self, df: pd.DataFrame) -> list:
        """create distortions for the dataset with the scaled customer_profile"""
        # prepare variables for looping
        feature_number: int = len(df.columns)
        distortions: list = []

        for k in range(1, feature_number + 1):
            kmeans = KMeans(
                n_clusters=k, init="k-means++", n_init="auto", random_state=0
            )
            kmeans.fit(df)
            distortions.append(kmeans.inertia_)

        return distortions

    def find_best_elbow(self, distortions: list) -> int:
        """
        Function to find the optimal k considered by distortions
        """
        array_distortions = np.array(distortions)
        # 1st derivative: graph slope
        slopes = np.divide(array_distortions[1:], array_distortions[:-1])
        # 2nd derivative: rate of change
        slope_changes = np.diff(slopes)

        # tmp variables used in slope calculation in the next process
        tmp_slopes = slopes.copy()
        tmp_slope_changes = slope_changes.copy()

        # find the most optimal k
        while True:
            try:
                # find the lowest slope change (the most linear index)
                min_slope_change_index = np.argmin(abs(tmp_slope_changes))

                # verifying if there's no lower slope after the most linear point
                if (
                    tmp_slope_changes[min_slope_change_index]
                    < tmp_slopes[min_slope_change_index:]
                ).all():
                    k_optimal = min_slope_change_index + 2
                else:
                    tmp_slope_changes[min_slope_change_index] = 1
                    continue
                return k_optimal
            except Exception as e:
                raise ValueError(e)

    def clustering(
        self, df: pd.DataFrame, scaled_df: pd.DataFrame, optimal_k: int
    ) -> pd.DataFrame:
        """
        Cluster inference,
        use 'scaled_df' as kmeans input and put the output into 'df' as a 'cluster' column name
        """
        kmeans = KMeans(
            n_clusters=optimal_k, init="k-means++", n_init="auto", random_state=0
        )
        output_df = df.copy()
        output_df["cluster"] = kmeans.fit(scaled_df).labels_

        return output_df, kmeans

    def process(self) -> dict:
        """Entrypoint to orchestrate the processes"""
        scaled_df, scaler = self.scale(df=self.df)

        distortions = self.train(df=scaled_df)
        optimal_k = self.find_best_elbow(distortions=distortions)

        output_df, trained_kmeans = self.clustering(
            df=self.df, scaled_df=scaled_df, optimal_k=optimal_k
        )

        output = {
            "output_df": output_df,
            "trained_kmeans": trained_kmeans,
            "fitted_scaler": scaler,
        }

        return output


class ClusterInterpretationService(BaseMLService):
    """
    Cluster Interpreter using LightGBM to consider permutation feature importance for each cluster.

    Methods
    -------
    - `process`: main process to orchestrate overall processes
        - `split_data`: split data for Interpreter training and evaluation
        - ~~`is_retrain_required`: check if new customer profile is explainable by the old trained interpreter~~
        - `train_interpreter`: train a new interpreter
        - `eval_trained_interpreter`: retrieve model fro searcher and calculate model performance
            - `log_evaluation`: calculate metrics of the interpreter
        - `interpret_cluster`: main process for interpretation logics
            - `calculate_important_score`: calculate feature importance score for each cluster from every features
            - `map_feature_importance`: find top 5 important feature for each cluster considered by important score and map feature names
            - `build_explode_cluster_df`: express the output as normalized DataFrame
            - `identify_anomaly_cluster`: flag *"is_anomaly"* in output DataFrame, and return flag for alerting
                - `exclude_unimportant_feature`: identify cluster(s) with no important feature

    Example
    -------
    ```python
    instance = ClusterInterpretationService()
    instance.get_input(df=df)
    output: dict = instance.process()
    ```
    """

    def __init__(self, df: pd.DataFrame):
        super().__init__()
        self.df = df
        self.X: pd.DataFrame
        self.y: pd.Series

    @staticmethod
    def log_evaluation(y_true, y_pred, float_point: int = 4) -> dict:
        eval_dict = {
            "date": datetime.today().date(),
            "f1_score_macro": round(
                f1_score(y_true=y_true, y_pred=y_pred, average="macro"), float_point
            ),
            "accuracy": round(
                accuracy_score(y_true=y_true, y_pred=y_pred), float_point
            ),
            "precision": round(
                precision_score(y_true=y_true, y_pred=y_pred, average="macro"),
                float_point,
            ),
            "recall": round(
                recall_score(y_true=y_true, y_pred=y_pred, average="macro"), float_point
            ),
            "interpreter_model": None,
            "interpreter_model_version": None,
            "interpreter_train_date": None,
        }

        # eval_df = pd.DataFrame({
        #     "date": datetime.today().date(),
        #     "f1_score_macro": round(f1_score(y_true=y_true, y_pred=y_pred, average="macro"), float_point),
        #     "accuracy": round(accuracy_score(y_true=y_true, y_pred=y_pred), float_point),
        #     "precision": round(precision_score(y_true=y_true, y_pred=y_pred, average="macro"), float_point),
        #     "recall": round(recall_score(y_true=y_true, y_pred=y_pred, average="macro"), float_point),
        #     "interpreter_model": None,
        #     "interpreter_model_version": None,
        #     "interpreter_train_date": None
        # }, index=[0])

        return eval_dict

    def split_data(
        self, df: pd.DataFrame, test_size: float = 0.2
    ) -> tuple[pd.DataFrame | pd.Series]:
        """Input: enriched_customer_profile with the cluster column"""
        self.X = df.drop(columns=["CustomerID", "cluster"])
        self.y = df["cluster"]

        X_train, X_test, y_train, y_test = train_test_split(
            self.X, self.y, test_size=test_size, random_state=0
        )

        return X_train, X_test, y_train, y_test

    def train_interpreter(self, X_train, y_train) -> RandomizedSearchCV:
        model = LGBMClassifier(verbose=-1)
        param_dist = {
            "n_estimators": [100, 200, 300, 500],
            "learning_rate": [0.1, 0.3, 0.5],
            "max_depth": [6, 15],
            "num_leaves": [63, 255],
            "lambda_l1": [0.5, 1],
            "lambda_l2": [0, 0.5],
            "min_data_in_leaf": [20, 500],
            "max_bin": [127, 255],
            "feature_fraction": [1],
            "subsample": [0.5, 1],
        }

        search = RandomizedSearchCV(
            estimator=model,
            param_distributions=param_dist,
            cv=10,
            scoring="f1_macro",
            random_state=0,
        )
        search.fit(X_train, y_train)

        return search

    def eval_trained_interpreter(
        self,
        trained_search: RandomizedSearchCV,
        X_test: pd.DataFrame,
        y_test: pd.DataFrame,
    ) -> dict:
        # get search output
        best_params = trained_search.best_params_
        best_estimator = trained_search.best_estimator_

        y_pred = best_estimator.predict(X_test)

        eval_scores = self.log_evaluation(y_true=y_test, y_pred=y_pred, float_point=4)

        output = {
            "best_estimator": best_estimator,
            "best_params": best_params,
            "eval_scores": eval_scores,
        }

        return output

    def calculate_important_score(self, tuned_model: LGBMClassifier) -> dict:
        """Calculate feature importance score for each cluster and each feature"""
        cluster_results: dict = {}
        for target in self.y.unique():
            result = permutation_importance(
                estimator=tuned_model,
                X=self.X[self.y == target],
                y=self.y[self.y == target],
                scoring="f1_macro",
                n_repeats=5,
                random_state=0,
            )
            cluster_results[target] = result

        return cluster_results

    def map_feature_importance(self, cluster_results: dict[str, dict]) -> list:
        """Find top 5 important feature for each cluster considered by important score"""
        mapped_cluster_factor: list = []
        for cluster, importance_score in cluster_results.items():
            # find top 5 important feature for each cluster considered by important score
            sorted_importances_idx = importance_score["importances_mean"].argsort()
            top_5_factor = list(self.X.columns[sorted_importances_idx][::-1][:5])
            feature_importance = np.round(
                importance_score["importances_mean"][sorted_importances_idx][::-1][:5],
                4,
            )

            # prepare as a DataFrame input
            mapped_cluster_factor.append(
                {
                    "cluster": cluster,
                    "important_feature": top_5_factor,
                    "score": feature_importance,
                }
            )

        return mapped_cluster_factor

    def build_explode_cluster_df(
        self, mapped_cluster_factor: list[dict]
    ) -> pd.DataFrame:
        """Build output DataFrame with all clusters: build_explode_important_feature_score_rank"""
        cluster_df = pd.DataFrame(mapped_cluster_factor).explode(
            column=["important_feature", "score"]
        )
        cluster_df["rank_important"] = cluster_df.groupby("cluster").cumcount() + 1
        return cluster_df

    def exclude_unimportant_feature(self, mapped_cluster_factor: list[dict]) -> list:
        """Keep only matter features (exclude importance score 0)"""
        post_mapped_cluster_factor: list = []
        for cluster in mapped_cluster_factor:
            if (cluster["score"] == 0).sum() == 5:  # top features
                continue
            else:
                # keep only matter features
                valid_features = (cluster["score"] != 0).sum()
                valid_columns = cluster["important_feature"][:valid_features]
                valid_scores = cluster["score"][:valid_features]

                post_mapped_cluster_factor.append(
                    {
                        "cluster": cluster["cluster"],
                        "important_feature": valid_columns,
                        "score": valid_scores,
                    }
                )

        return post_mapped_cluster_factor

    def identify_anomaly_cluster(
        self, cluster_df: pd.DataFrame, mapped_cluster_factor: list[dict]
    ) -> tuple[pd.DataFrame, bool]:
        # prepare a component
        post_mapped_cluster_factor = self.exclude_unimportant_feature(
            mapped_cluster_factor=mapped_cluster_factor
        )

        # identify anomaly cluster
        if len(post_mapped_cluster_factor) == len(mapped_cluster_factor):
            logging.info("Anomaly cluster is not found")
            cluster_df["is_anomaly"] = False
            is_anomaly_exist = False
        else:
            logging.warning("Anomaly cluster is found")
            all_cluster = set([cluster["cluster"] for cluster in mapped_cluster_factor])
            cluster_anomaly_removed = set(
                [cluster["cluster"] for cluster in post_mapped_cluster_factor]
            )
            anomaly_cluster = list(all_cluster - cluster_anomaly_removed)

            cluster_df["is_anomaly"] = cluster_df.apply(
                {"cluster": lambda x: True if x in anomaly_cluster else False}
            )

            logging.info(f"Anomaly cluster: {anomaly_cluster}")
            is_anomaly_exist = True

        return cluster_df, is_anomaly_exist

    def interpret_cluster(self, tuned_model: LGBMClassifier) -> pd.DataFrame:
        """Interpret Cluster Behavior by permutation feature importance using `enriched_customer_profile`"""
        cluster_results = self.calculate_important_score(tuned_model=tuned_model)
        mapped_cluster_factor = self.map_feature_importance(
            cluster_results=cluster_results
        )
        cluster_df = self.build_explode_cluster_df(
            mapped_cluster_factor=mapped_cluster_factor
        )
        cluster_df, is_anomaly_exist = self.identify_anomaly_cluster(
            cluster_df=cluster_df, mapped_cluster_factor=mapped_cluster_factor
        )
        return cluster_df, is_anomaly_exist

    # for triggering model retraining
    def is_retrain_required():
        # TODO: solve this logic
        return False

    def process(self) -> dict:
        X_train, X_test, y_train, y_test = self.split_data(df=self.df, test_size=0.2)
        trained_search = self.train_interpreter(X_train=X_train, y_train=y_train)
        output_eval_trained_interpreter: dict = self.eval_trained_interpreter(
            trained_search=trained_search,
            X_test=X_test, 
            y_test=y_test
        )
        best_estimator = output_eval_trained_interpreter.get("best_estimator")
        best_params = output_eval_trained_interpreter.get("best_params")
        eval_scores = output_eval_trained_interpreter.get("eval_scores")

        cluster_df, is_anomaly_exist = self.interpret_cluster(
            tuned_model=best_estimator
        )

        output = {
            "cluster_df": cluster_df,
            "best_estimator": best_estimator,
            "best_params": best_params,
            "eval_scores": eval_scores,
            "is_anomaly_exist": is_anomaly_exist,
        }

        return output


class MlProcessor(AbstractMLProcessor):
    def __init__(
        self,
        df: pd.DataFrame
    ):
        super().__init__()
        self.df = df.copy()

    @property
    def __str__(self):
        bases = [base.__name__ for base in self.__class__.__bases__]
        bases.append(self.__class__.__name__)
        return ".".join(bases)

    def process(self) -> dict:
        """Wrapper Orchestrate all processes and ml services"""

        enriched_customer_profile: pd.DataFrame = CustomerProfilingService(df=self.df).process()
        output_segmenter: dict = CustomerSegmentationService(df=enriched_customer_profile).process()
        output_interpreter: dict = ClusterInterpretationService(df=output_segmenter.get("output_df")).process()
        
        output = {
            "df_cluster_rfm": output_segmenter.get("output_df"),
            "df_cluster_importance": output_interpreter.get("cluster_df"),
            "segmenter_trained": output_segmenter.get("trained_kmeans"),
            "segmenter_scaler": output_segmenter.get("fitted_scaler"),
            "interpreter": output_interpreter.get("best_estimator"),
            "interpreter_params": output_interpreter.get("best_params"),
            "interpreter_metrics": output_interpreter.get("eval_scores"),
            "is_anomaly_exist": output_interpreter.get("is_anomaly_exist"),
        }

        return output