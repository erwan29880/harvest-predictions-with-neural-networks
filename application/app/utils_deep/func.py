import pandas as pd
import numpy as np 
from datetime import datetime, date
from bdd.conn.connection import Conn 
from typing import Tuple


def to_time(x) -> datetime:
    """date or string to datetime"""
    if isinstance(x, date):
        n = x.strftime('%Y-%m-%d')
        n = datetime.strptime(n, '%Y-%m-%d')
        return n
    else:
        n = datetime.strptime(x, '%Y-%m-%d')
        return n


def load_data() -> Tuple[(pd.DataFrame, np.ndarray, pd.DataFrame)]:
    connect = Conn()
    data = connect.flux_neural_network()
    data = data.rename(columns={'jour':'date_recolte', 't_max':'tmax', 't_min':'tmin'})
    
    data['date_recolte'] = data['date_recolte'].apply(lambda x: to_time(x))
    data = data.sort_values(by='date_recolte').reset_index(drop=True)
    
    # decompose dates
    data['day'] = data['date_recolte'].dt.day
    data['weekday'] = data['date_recolte'].dt.dayofweek
    data['weekday'] = data['weekday']
    data['weekday2'] = data['weekday'] + 5
    data['annee'] = data['date_recolte'].dt.dayofyear
    data['annee2'] = data['annee'] + 365 - 50
    data['annee3'] = data['annee'] + 365 - 100

    samedis = []
    for i in range(len(data)):
        if data.loc[i, 'weekday'] == 5:
            samedis.append(1)
        else:
            samedis.append(-1)
    data['samedis'] = samedis

    # cos & sin with dates
    data['day_cos'] = data['day'].apply(lambda x: np.cos( x*(2 * np.pi/30.5)))
    data['day_sin'] = data['day'].apply(lambda x: np.sin( x*(2 * np.pi/30.5)))
    data['annee_cos'] = data['annee2'].apply(lambda x: np.cos( x*(2 * np.pi/365.25)))
    data['annee_sin'] = data['annee3'].apply(lambda x: np.sin( x*(2 * np.pi/365.25)))
    data['weekday_sin'] = data['weekday2'].apply(lambda x: np.sin( x*(2*np.pi/7)))
    data['weekday_cos'] = data['weekday'].apply(lambda x: np.cos( x*(2* np.pi/7)))
    
    # for plotting 
    data = data.set_index('date_recolte')
    
    # feature selection
    data = data[['qte', 
                 'tmax', 
                 'tmin', 
                 'ctmin', 
                 'ctmax', 
                 'cpluie', 
                 'weekday_cos', 
                 'weekday_sin', 
                 'day_cos', 
                 'day_sin', 
                 'annee_cos', 
                 'annee_sin', 
                 'samedis']]
    target = np.array(data['qte']).reshape(-1, 1)
    features = data.copy()
    features2 = data.drop(['qte'], axis=1)
    
    return features, target, features2


def load_data_week() -> Tuple[(pd.DataFrame, np.ndarray, pd.DataFrame)]:
    connect = Conn()
    data = connect.flux_neural_network2()
    data = data.drop(['id', 'an'], axis=1)
    data.columns = ['semaine', 'qte', 'tmin', 'tmax', 'pluie', 'ctmax', 'ctmin', 'cpluie']
    data['semaine2'] = data['semaine']+4

    # sin and cos features
    data['mois_cos'] = data['semaine'].apply(lambda x: np.cos( x*(2 * np.pi/12)))
    data['mois_sin'] = data['semaine'].apply(lambda x: np.sin( x*(2 * np.pi/12)))
    data['semaine_cos'] = data['semaine'].apply(lambda x: np.cos( x*(2 * np.pi/52.2)))
    data['semaine_sin'] = data['semaine'].apply(lambda x: np.sin( x*(2 * np.pi/52.2)))
    data['semaine_sin2'] = data['semaine2'].apply(lambda x: np.sin( x*(2 * np.pi/52.2)))
    data = data.drop('semaine2', axis=1)
    
    target = np.array(data['qte']).reshape(-1, 1)
    features=data.copy()
    features2=data.drop(['qte'], axis=1)
    
    return features, target, features2



