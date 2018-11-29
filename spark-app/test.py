from flask import Flask
from pyspark.ml.recommendation import ALS

app = Flask(__name__)


@app.route('/', methods=["GET"])
def hello():
    return "YOYO"


model = ALS(userCol="userId", itemCol="movieId", ratingCol="rating").fit(ratings)

if __name__ == '__main__':
    app.run()

{
    "m_cat_count": [
        [
            "Film & Video",
            3453
        ]
    ],
    "m_cat_count_state": [
        [
            "Film & Video",
            "successful",
            1452
        ],
        [
            "Film & Video",
            "failed",
            2001
        ]
    ],
    "m_cat_sum_goals": [
        [
            "Film & Video",
            175266322.5700002
        ]
    ],
    "prediction": [
        {
            "prediction": "failed"
        }
    ],
    "state_count": [
        [
            "failed",
            11867
        ],
        [
            "successful",
            8133
        ]
    ]
}
