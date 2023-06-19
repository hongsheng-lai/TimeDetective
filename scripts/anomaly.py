from collections import deque
from darts import TimeSeries
from darts.dataprocessing.transformers import Scaler
import numpy as np
from darts.ad import QuantileDetector, ThresholdDetector

class AnomalyDetector:
    def __init__(self, lag=1, num_models=1, num_samples=1):
        self.models = deque(maxlen=lag*num_models)
        self.len = lag*num_models
        self.num_samples = num_samples

    def add_filter(self, train=None, scorer=None, detector=None):
        """
        Add a new anomaly detector to the queue.
        """
        if train is None:
            raise ValueError("Must provide training data to add a detector to the queue.")
        
        # Chekch train data type
        if not isinstance(train, TimeSeries):
            raise TypeError("Training data must be a TimeSeries object.")
        
        model = {}
        
        # Fit the scorer
        if scorer is not None :
            scorer.fit(train)
            model['scorer'] = scorer
        else:
            raise ValueError("Must provide a scorer to add to the queue.")

        # Fit the detector and add to the queue
        if detector is not None :
            if isinstance(detector, QuantileDetector):  # Only QuantileDetector needs to fit from the training data
                detector.fit(scorer.score(train))
            # detector.fit(scorer.score(train))
            model['detector'] = detector
        else:
            raise ValueError("Must provide a detector to add to the queue.")
        
        self.models.append(model)
        
    def add_forecaster(self, train=None, forecaster=None, scorer=None, detector=None, ratio=0.7):
        if train is None:
            raise ValueError("Must provide training data to add a detector to the queue.")
        
        # Chekch train data type
        if not isinstance(train, TimeSeries):
            raise TypeError("Training data must be a TimeSeries object.")

        model = {}    
        
        # Split and Normalize
        train, val = train.split_before(ratio)
        transformer = Scaler()
        train_transformed = transformer.fit_transform(train)
        val_transformed = transformer.transform(val)

        if forecaster is not None :
            forecaster.fit(train_transformed)
            pred = forecaster.predict(n=len(val_transformed), num_samples=self.num_samples) # 要往後預測多少 加 num_samples=1000 才有quantile
            model['forecaster'] = forecaster
        else:
            raise ValueError("Must provide a forecaster to add to the queue.")

        # Fit the scorer
        if scorer is not None :
            scorer.fit_from_prediction(val_transformed, pred) # 這邊要丟進來的新資料
            model['scorer'] = scorer
        else:
            raise ValueError("Must provide a scorer to add to the queue.")

        # Fit the detector and add to the queue
        if detector is not None :
            detector.fit(scorer.score_from_prediction(val_transformed, pred))
            model['detector'] = detector
        else:
            raise ValueError("Must provide a detector to add to the queue.")
        
        self.models.append(model)

    def detect_anomalies(self, test, type="or"):
        """
        Detect anomalies in the given data using all scorers in the queue.
        """
        # Create a list of binary anomaly values to fit the biwtwise type
        if type == "or":
            binary_anom_array = np.zeros(len(test), dtype=bool)
        elif type == "and":
            binary_anom_array = np.ones(len(test), dtype=bool)
        else:
            raise ValueError("Invalid type: {}".format(type))
        
        
        for i in range(self.len):
            model = self.models[i]

            if 'forecaster' in model:
                forecaster = model['forecaster']
                scorer = model['scorer']
                detector = model['detector']

                pred = forecaster.predict(n=len(test), num_samples=self.num_samples)
                anom_score = scorer.score_from_prediction(test, pred)
                binary_anom = detector.detect(anom_score)
                binary_anom = np.ravel(binary_anom.values()).astype(bool) # Convert to 1D array
            else:
                scorer = model['scorer']
                detector = model['detector']

                anom_score = scorer.score(test)
                binary_anom = detector.detect(anom_score)
                binary_anom = np.ravel(binary_anom.values()).astype(bool) # Convert to 1D array

            # Fix window size for kmeans
            if len(test) > len(binary_anom):
                binary_anom = np.pad(binary_anom, (0, len(test)-len(binary_anom)), 'constant', constant_values=(False, False))
                # binary_anom = np.pad(binary_anom, (0, len(binary_anom)), 'constant', constant_values=(False, False))

            # Bitwise OR
            if type == "or":
                binary_anom_array |= binary_anom
            # Bitwise AND
            elif type == "and":
                binary_anom_array &= binary_anom
            else:
                raise ValueError("Invalid type: {}".format(type))
            
        return binary_anom_array