import pandas as pd
from transformers import pipeline
from pyspark.sql.functions import pandas_udf
from pyspark.sql import SparkSession
from typing import List

class ZeroShotClassifier:
    
    name: str
    model_name: str
    candidate_labels: List[str]

    def __init__(self, name: str, model_name: str, candidate_labels: List[str], spark: SparkSession):
        self.name = name
        self.model_name = model_name
        self.candidate_labels = candidate_labels

        self.udf_name = f"{self.name}_udf"
        self.classify_udf = self.build_udf()
        self.classify_function = self.classify_udf.__wrapped__ # Get the Series-to-Series function

        spark.udf.register(self.udf_name, self.classify_udf)

    def build_udf(self):
        # this could also be done with default_factory
        zero_shot_pipeline = pipeline(
            task="zero-shot-classification",
            model=self.model_name,
        )

        @pandas_udf("array<struct<class_name string, score double>>")
        def classify_batch_udf(texts: pd.Series) -> pd.Series:
            """UDF that classifies a batch of texts using the zero-shot-classification
            pipeline. The pipeline is loaded once and then used for all calls to the UDF.
            Args:
                texts (pd.Series): Series of texts to classify
            Returns:
                pd.Series: Series of classification results
            """
            pipe = zero_shot_pipeline(
                    texts.to_list(),
                    truncation=True,
                    candidate_labels=self.candidate_labels,
            )
            output = []

            for result in pipe:
                row = []
                for i, label in enumerate(result['labels']):
                    row.append({'class_name': label, 'score': result['scores'][i]})
                output.append(row)

            return pd.Series(output)
        
        return classify_batch_udf