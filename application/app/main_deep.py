import pandas as pd
import numpy as np 
import os
import joblib

import tensorflow as tf 
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics import mean_absolute_error
from keras.models import Sequential, load_model, Model
from keras.layers import Dense, Flatten, LSTM, Input, Concatenate, Dropout, Conv1D, Reshape, Add, TimeDistributed
from keras.metrics import MeanAbsoluteError 
from keras.losses import MeanSquaredError
from keras.optimizers import Adam 

from utils_deep import func
from utils_deep.logs import logger
from typing import Tuple 
from datetime import date, timedelta, datetime
from bdd.conn.connection import Conn 


class ML:
    
    """
    Training class 
        - models based on LSTM et Convolutions layers
        - datasets are prepared in postreSQL functions and in func file
    """
    
    def __init__(self) -> None:
        self.features, self.target, self.features2 = func.load_data()        
        self.scaler_features = "./scalers/scaler_features1.joblib"
        self.scaler_features2 = "./scalers/scaler_features2.joblib"
        self.scaler_target = "./scalers/scaler_target.joblib"
        self.model1 = './models_deep/model_12_6_add.h5'
        self.model2 = 'model_feature4.h5'
        self.for_pred1 = None
        self.for_pred2 = None

        self.features_week, self.target_week, self.features2_week = func.load_data_week()        
        self.scaler_features1_week = "./scalers/scaler_features1_week.joblib"
        self.scaler_features2_week = "./scalers/scaler_features2_week.joblib"
        self.scaler_target_week = "./scalers/scaler_target_week.joblib"
        self.model1_week = './models_deep/model_feature1_week.h5'
        self.model2_week = 'model_feature_week.h5'
        self.for_pred1_week = None
        self.for_pred2_week = None
        
        self.connect = Conn()
        self.history = None

        
    def scale(self, x: np.ndarray, fit: bool, path: str) -> np.ndarray:
        """Scaling features and target in range -1, 1
            Due to good temporality et no trend, box-cox for target is less accurate"""
        
        assert isinstance(x, np.ndarray)
        assert x.shape[0] > 0
        
        if fit:
            scaler = MinMaxScaler(feature_range=(-1, 1)).fit(x)
            joblib.dump(scaler, path)
        assert os.path.exists(path)
        return joblib.load(path).transform(x)
    
    
    def inverse_scale(self, x: np.ndarray, path: str) -> np.ndarray:
        """inverse scaling for target"""
        assert isinstance(x, np.ndarray)
        assert x.shape[0] > 0
        assert os.path.exists(path)
        return joblib.load(path).inverse_transform(x)
    
                    
    def prepare_temporal_dataset(self, 
                                 features: np.ndarray, 
                                 features2: np.ndarray, 
                                 target: np.ndarray, 
                                 step: int) -> Tuple[(np.ndarray, np.ndarray, np.ndarray)]:
        """LSTM like dataset, with a step of 6 : total data, steps, num of features, one dim"""
        assert features.shape[0] > 6
        assert target.shape[0] >= 6
        i=12
        X, X2, Y = [], [], []
        while i < len(features)-6:
            feat = features[i-12:i]
            feat2 = features2[i+3:i+9]
            targ = target[i+3:i+9]
            X.append(feat)
            X2.append(feat2)
            Y.append(targ)
            i+=step
        X = np.array(X)
        X2 = np.array(X2)
        Y = np.array(Y)

        if step == 6:
            self.for_pred1 = np.expand_dims(X[-1], 0)
            self.for_pred2 = np.expand_dims(X2[-1], 0)
  
            X = X[:-1]
            X2 = X2[:-1]
            Y = Y[:-1]

        return X, X2, Y
    

    def prepare_temporal_dataset_week(self, 
                                      features: np.ndarray, 
                                      features2: np.ndarray, 
                                      target: np.ndarray) -> Tuple[(np.ndarray, np.ndarray, np.ndarray)]:
        """7 weeks in the past, 
        without current week (preds are on thursday for next week, so we don't actually know the total crop for the current week) 
        to predict next one
        """
        X, X2, Y = [], [], []
        windows = 7
        i = windows
        while i <= len(target)-1:
            feat = features[i-windows:i-1]
            feat2 = features2[i]
            targ = target[i]
            X.append(feat)
            X2.append(feat2)
            Y.append(targ)
            i+=1

        X = np.array(X)
        X2 = np.array(X2)
        Y = np.array(Y)

        self.for_pred1_week = np.expand_dims(X[-1], 0)
        self.for_pred2_week = np.expand_dims(X2[-1], 0)
        X = X[:-1]
        X2 = X2[:-1]
        Y = Y[:-1]
      
        return X, X2, Y


    def _model(self, 
               X: np.ndarray, 
               X2: np.ndarray, 
               Y: np.ndarray, 
               test=False) -> None:   
        """train a model or use a pretrained one with a warmup """
        
        if not os.path.exists(self.model1):
            input1_ = Input(X.shape[1:], name='input1')
            lstm1 = LSTM(128, return_sequences=True, dropout=0.15, name='lstm64_1_1')(input1_)
            x = LSTM(128, return_sequences=True, dropout=0.15, name='lstm64_1_2')(lstm1)
            x = LSTM(6, dropout=0.15, name='lstm6_1_1')(x)
            x = Reshape((6,1), name='reshape_1')(x)
            x = TimeDistributed(Dense(1), name='Timedistributed_1')(x)

            input2_ = Input(X2.shape[1:], name='input2')
            lstm_meteo1 = LSTM(64, return_sequences=True, dropout=0.15, name='lstm64_2_1')(input2_)
            z = LSTM(64, return_sequences=True, dropout=0.15, name='lstm64_2_2')(lstm_meteo1)
            z = LSTM(1, return_sequences=True, dropout=0.15, name='lstm1_2')(z)
            z = TimeDistributed(Dense(1), name='Timedistributed_2')(z)

            c = Add(name='Concat')([x, z])
            c = Dense(128, activation='tanh', name='Dense128_1_tanh')(c)
            c = Dropout(0.15, name='dropout_0.15')(c)
            c = Dense(128, activation='tanh', name='Dense128_2_tanh')(c)

            c = TimeDistributed(Dense(1, name='Dense1_1_linear'))(c)
            model = Model([input1_, input2_], c)

            model.compile(optimizer=Adam(learning_rate=0.0005), 
                          loss=tf.keras.losses.MeanSquaredError())
            cp = tf.keras.callbacks.ModelCheckpoint(self.model1, 
                                                    save_best_only=True, 
                                                    mode='min', 
                                                    verbose=1, 
                                                    monitor='loss')
            ea = tf.keras.callbacks.EarlyStopping(patience=20, 
                                                  monitor='loss', 
                                                  verbose=1, 
                                                  restore_best_weights=True, 
                                                  start_from_epoch=150)
            rlr = tf.keras.callbacks.ReduceLROnPlateau(monitor='loss', 
                                                       factor=0.1, 
                                                       patience=150)
            self.history = model.fit([X, X2], 
                                     Y, 
                                     epochs=1000, 
                                     batch_size=8, 
                                     callbacks=[ea, cp, rlr])

        else:
            model = load_model(self.model1)
            cp = tf.keras.callbacks.ModelCheckpoint(self.model2, 
                                                    save_best_only=True, 
                                                    mode='min', 
                                                    verbose=1, 
                                                    monitor='loss')
            ea = tf.keras.callbacks.EarlyStopping(patience=20, 
                                                  monitor='loss', 
                                                  verbose=1, 
                                                  restore_best_weights=True, 
                                                  start_from_epoch=80)
            rlr = tf.keras.callbacks.ReduceLROnPlateau(monitor='loss', 
                                                       factor=0.1, 
                                                       patience=80)
            self.history = model.fit([X, X2], 
                                     Y, 
                                     epochs=1000, 
                                     batch_size=8, 
                                     callbacks=[ea, cp, rlr])
                

    def _model_week(self, shape1: tuple, shape2: tuple) -> Model:
        input1_ = Input(shape=shape1)
        input2_ = Input(shape=shape2)

        # branch1
        lstm1 = LSTM(64, return_sequences=True)(input1_)
        lstm1 = Reshape((6, 64, 1))(lstm1)
        lstm1 = Conv1D(64, (2), strides=1, padding="same", activation='relu')(lstm1)
        lstm1 = Dropout(0.2)(lstm1)
        lstm1 = Conv1D(64, (2), strides=2, padding="same", activation='relu')(lstm1)
        lstm1 = Dropout(0.2)(lstm1)
        lstm1 = Conv1D(64, (2), strides=2, padding="same", activation='relu')(lstm1)
        lstm1 = Flatten()(lstm1)

        # branch2
        lstm2 = Dense(256, activation='relu')(input2_)
        lstm2 = Reshape((16, 16, 1))(lstm2)
        lstm2 = Conv1D(128, 3, strides=2, padding="same", activation='relu')(lstm2)
        lstm2 = Dropout(0.2)(lstm2)
        lstm2 = Conv1D(128, 3, strides=2, padding="same", activation='relu')(lstm2)
        lstm2 = Dropout(0.2)(lstm2)
        lstm2 = Conv1D(64, 3, strides=2, padding="same", activation='relu')(lstm2)
        lstm2 = Conv1D(64, 3, strides=2, padding="same", activation='relu')(lstm2)
        lstm2 = Flatten()(lstm2)

        # concat
        concat = Concatenate()([lstm1, lstm2])
        dense1 = Dropout(0.3)(concat)
        dense1 = Dense(3000, activation='relu')(dense1)
        dense1 = Dropout(0.3)(dense1)
        dense1 = Dense(1000, activation='relu')(dense1)
        dense1 = Dense(300, activation='relu')(dense1)
        dense1 = Dense(32, activation='relu')(dense1)
        dense1 = Dropout(0.3)(dense1)
        output_ = Dense(1)(dense1)

        model = Model(inputs=[input1_, input2_], outputs=[output_])
        model.compile(optimizer=Adam(learning_rate=0.0005), loss=MeanSquaredError())

        return model


    def _metrics(self, method: str) -> None:
        perte = 'val_loss' if 'val_loss' in pd.DataFrame(self.history.history).columns else 'loss'
        mse = int(self.inverse_scale(np.array(np.min(pd.DataFrame(self.history.history)[perte]))
                                     .reshape(-1, 1), self.scaler_target))
        rmse = np.sqrt(mse)
        logger(f'rmse model 1 {int(rmse)}', 'métrique', method)

            
    def train_features(self, step: int) -> None:
        """day model training"""

        # load_data
        X = self.scale(np.array(self.features), 'fit', self.scaler_features)
        X2 = self.scale(np.array(self.features2), 'fit', self.scaler_features2)
        Y = self.scale(self.target, 'fit', self.scaler_target) 

        X, X2, Y = self.prepare_temporal_dataset(X, X2, Y, step)
        
        # train
        self._model(X, X2, Y)
        self._metrics('run final day')
  

    def train_features_week(self, initial: bool) -> None:
        X = self.scale(np.array(self.features_week), 'fit', self.scaler_features1_week)
        X2 = self.scale(np.array(self.features2_week), 'fit', self.scaler_features2_week)
        Y = self.scale(self.target_week, 'fit', self.scaler_target_week) 
        X, X2, Y = self.prepare_temporal_dataset_week(X, X2, Y)
        
        # load model et (re)train
        model = self._model_week(X.shape[1:], X2.shape[1:]) if initial else load_model(self.model1_week)

        if initial:
            # first train
            cp = tf.keras.callbacks.ModelCheckpoint(self.model2_week, 
                                                    save_best_only=True, 
                                                    mode='min', 
                                                    verbose=1)
            es = tf.keras.callbacks.EarlyStopping(monitor='val_loss', 
                                                  mode='min', 
                                                  patience=12)
            self.history = model.fit([X, X2], 
                                     Y, 
                                     validation_split=0.5, 
                                     epochs=500, 
                                     batch_size=4, 
                                     callbacks=[cp, es], 
                                     shuffle=False)
            self._metrics('run 0.5 week')

            # second train
            cp = tf.keras.callbacks.ModelCheckpoint(self.model2_week, 
                                                    save_best_only=True, 
                                                    mode='min', 
                                                    verbose=1)
            es = tf.keras.callbacks.EarlyStopping(monitor='val_loss', 
                                                  mode='min', 
                                                  patience=20)
            self.history = model.fit([X, X2], 
                                     Y, 
                                     validation_split=0.2, 
                                     epochs=500, 
                                     batch_size=4, 
                                     callbacks=[cp, es], 
                                     shuffle=False)
            self._metrics('run 0.2 week')

        cp = tf.keras.callbacks.ModelCheckpoint(self.model2_week, 
                                                save_best_only=True, 
                                                mode='min', 
                                                verbose=1, 
                                                monitor='loss')
        es = tf.keras.callbacks.EarlyStopping(monitor='loss', 
                                              mode='min', 
                                              patience=10, 
                                              start_from_epoch=40)
        self.history = model.fit([X, X2], 
                                 Y,  
                                 epochs=1000, 
                                 batch_size=4, 
                                 callbacks=[cp, es], 
                                 shuffle=False)
        self._metrics('run final week')



    def predict(self) -> Tuple[(np.ndarray, np.ndarray)]:
        """use pretrained model and retrain with new data, then predict"""
        self.train_features(step=6)
        self.train_features_week(initial=False)
        pred1 = load_model(self.model2_week).predict([self.for_pred1_week, self.for_pred2_week])
        pred2 = load_model(self.model2).predict([self.for_pred1, self.for_pred2])
        
        #inverse scaling
        pred1 = self.inverse_scale(pred1.flatten()
                                   .reshape(-1, 1), self.scaler_target_week).flatten().astype(int)
        pred2 = self.inverse_scale(pred2.flatten().reshape(-1, 1), self.scaler_target).flatten().astype(int)
        
        return pred1, pred2
        
        
        
    def rec_bdd(self) -> None:
        """create a temporal index from next monday to saturday"""
        today = date.today()
        assert today.weekday() == 3  # thursday in python
        dates_list = [today+timedelta(days=4) + timedelta(days=i) for i in range(6)]
        
        # prédictions
        pred1, pred2 = self.predict() 
        
        # make sure all days have predictions
        df = pd.DataFrame(list(zip(dates_list, pred2)), columns=['date', 'pred2']).fillna(0)
        assert df.shape[0] == 6
        
        # insert predictions in database
        sql_list = []
        for i in range(6):  
            pr2 = df.loc[i, 'pred2']
            if pr2 < 0:
                pr2 = 0      
            sql = """insert into cf_pred_neural_networks(jour, pred2)values('{}', {})""".format(df.loc[i, 'date'], pr2)
            sql_list.append(sql)
        
        sql = """update cf_pred_neural_networks set pred1={} where jour='{}';""".format(pred1[0], dates_list[0])
        sql_list.append(sql)
            
        self.connect.sql_list(sql_list)
            
    
   
if __name__=="__main__":
    ml = ML()
    ml.rec_bdd()