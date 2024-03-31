from census.entity.config_entity import DataIngestionConfig
from census.exception import CensusException
from census.logger import logging
from census.entity.artifact_entity import DataIngestionArtifact
import sys,os
import tarfile #To extract zip file
from six.moves import urllib #To download data
import pandas as pd
import numpy as np
from sklearn.model_selection import StratifiedShuffleSplit
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from census.constant import *


class DataIngestion:

    def __init__(self, data_ingestion_config:DataIngestionConfig):
        try:
            logging.info(f"{'='*20} Data Ingestion log Started {'='*20}")
            self.data_ingestion_config = data_ingestion_config
        except Exception as e:
            raise CensusException(e,sys) from e

    def load_census_data(self) -> str:
        try:
            client_id = self.data_ingestion_config.database_client_id
            client_secret = self.data_ingestion_config.database_client_secret
            # connecting to cassandra database
        
            # Set up the Cassandra cluster and authentication
            cloud_config = {'secure_connect_bundle': r'E:\Machine_Learning_Project\secure-connect-adult-census-data.zip'}
            auth_provider = PlainTextAuthProvider(client_id, client_secret)
            cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
            session = cluster.connect()

            #session.execute("USE income;")

            dataframe = pd.DataFrame(list(session.execute("select * from income.adult_census_data;")))

            raw_data_dir = self.data_ingestion_config.raw_data_dir

            if os.path.exists(raw_data_dir):
                os.path.remove(raw_data_dir)

            os.makedirs(raw_data_dir,exist_ok=True)

            raw_data_file_path = os.path.join(raw_data_dir,RAW_DATA_FILE_NAME)
            logging.info(f"Extracting Data from Database into : [{raw_data_file_path}]")
            
            dataframe.to_csv(raw_data_file_path, index = False)
            logging.info(f"Extraction completed.")  
             
            return raw_data_file_path
        except Exception as e:
            raise CensusException(e,sys) from e

    
    def split_data_as_train_test(self) -> DataIngestionArtifact:
        try:
            raw_data_dir = self.data_ingestion_config.raw_data_dir

            file_name = os.listdir(raw_data_dir)[0]

            census_file_path = os.path.join(raw_data_dir,file_name)

            census_data_frame = pd.read_csv(census_file_path)

            strat_train_set = None
            strat_test_set = None

            split = StratifiedShuffleSplit(n_splits = 1 , test_size = 0.2, random_state = 2)

            for train_index, test_index in split.split(census_data_frame,census_data_frame["salary"]):
                strat_train_set = census_data_frame.loc[train_index]
                strat_test_set = census_data_frame.loc[test_index]

            train_file_path = os.path.join(self.data_ingestion_config.ingested_train_dir, file_name)
            test_file_path = os.path.join(self.data_ingestion_config.ingested_test_dir, file_name)
        
            if strat_train_set is not None:
                os.makedirs(self.data_ingestion_config.ingested_train_dir, exist_ok = True)
                logging.info(f"Exporting training dataset to file: [{train_file_path}]")
                strat_train_set.to_csv(train_file_path, index = False)       
        
            if strat_test_set is not None:
                os.makedirs(self.data_ingestion_config.ingested_test_dir, exist_ok = True)
                logging.info(f"Exporting test dataset to file: [{test_file_path}]")
                strat_test_set.to_csv(test_file_path, index = False)       

            data_ingestion_artifact = DataIngestionArtifact(train_file_path=train_file_path,
                                                            test_file_path=test_file_path,
                                                            is_ingested=True,
                                                            message="Data Ingestion completed sucessfully."
                                                            )
            logging.info(f"Data ingestion artifact : [{data_ingestion_artifact}]") 
            return data_ingestion_artifact                                                  
        except Exception as e:
            raise CensusException(e,sys) from e

            
    def initiate_data_ingestion(self) -> DataIngestionArtifact:
        try:
            raw_data_file_path = self.load_census_data()
            return self.split_data_as_train_test()
        except Exception as e:
            raise CensusException(e,sys) from e

    def __del__(self):
        logging.info(f"{'='*20} Data Ingestion log completed {'='*20} \n\n")
            