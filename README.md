# TimeDetective
Comprehensive Anomaly Detection pipeline for Time Series Database with Darts

## Installation
```
pip install darts
```
If you want to use the Deep Learning models
```
pip install -r torch.txt
```
If you missing some dependencies, you can install them with
```
pip install -r core.txt
```
## Explanation
**anomaly.AnomalyDetector(self, lag=1, num_models=1, num_samples=1)**

lag: The number of the previous stages the model store
num_models: The number of the models that the model store, can be filter method or forecast method
num_samples: In some forecast method, the model can sample multiple times and use mean to get the final result

**add_filter(self, train=None, scorer=None, detector=None)**

train: The training data, should be a TimeSeries object
scorer: The scorer used to score the model, should be a Scorer object
detector: The detector used to detect the anomaly, should be a Detector object

**add_forecaster(self, train=None, forecaster=None, scorer=None, detector=None, ratio=0.7)**

train: The training data, should be a TimeSeries object
forecaster: The forecaster used to forecast the data, should be a Forecaster object
scorer: The scorer used to score the model, should be a Scorer object
detector: The detector used to detect the anomaly, should be a Detector object
ratio: The ratio of the training data used to train the forecaster

**detect_anomalies(self, test, type="or")**

test: The testing data, should be a TimeSeries object
type: The type of the bitwise operator for voting method, can be "or" or "and"

## Usage
The example code is in  `scripts`
We need to run two scripts at the same time
```
python3 scripts/InsertToDB.py   # Insert the data to the database one by one with a time interval (just like real sensor)
python3 scripts/ReadFromDB.py   # Read the data from the database and detect the anomaly (contain user define block)
```

For more model, for example RNN model, you can check the [Darts](https://unit8co.github.io/darts/index.html) documentation
Noted that some of the need GPU resources, make sure you meet the requirements