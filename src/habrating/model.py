import pickle
import asyncio
import platform
from sklearn.ensemble import RandomForestRegressor
from sklearn.utils import shuffle

from . import db, parser


class HabrHubRatingRegressor:
    def __init__(self, hub_name):
        """
        Create new rating regressor
            :param hub_name: name of hub for rating regression
        """
        self.estimator = RandomForestRegressor(n_estimators = 100, n_jobs=-1, verbose=2)
        self.hub_name = hub_name
        self.text_transformer = None
        self.title_transformer = None
        

    def fit(self, X_train, y_train):
        """
        Fit model
            :param X_train: features for training
            :pararm y_train: answers for training
        """
        self.estimator.fit(X_train, y_train)

    def predict(self, X):
        """
        Predict answer from features
            :param X: features data
        """
        return self.estimator.predict(X)

    def predict_by_urls(self, urls):
        """
        Predict rating from urls
            :param urls: array of url from target hub (must equals to model hub name)
        """
        loop = asyncio.get_event_loop()
        posts = list(map(lambda url: parser.parse_article(url), urls))
        return self.predict_by_posts(posts)

    def predict_by_posts(self, posts):
        """
        Predict rating by posts data
            :param posts: array of parsed post data
        """
        for post in posts:
            db.vectorize_post(post, self.text_transformer, self.title_transformer)
        X, _ = db.cvt_to_DataFrames(posts)
        y_predict = self.predict(X)
        return y_predict

    def set_transformers(self, text_transformer, title_transformer):
        """
        Set body and title transformers, needed for predicting by parsed post data
        """
        self.text_transformer = text_transformer
        self.title_transformer = title_transformer

    def save(self, file_path = None):
        """
        Save model data to file
            :param file_path: path to model file. Default name is hub name with extention .hubmodel32 or
            .hubmodel64 (according computer architecture)
        """
        if file_path is None:
            arch = platform.architecture()[0].replace('bit','')
            file_path = self.hub_name+'.hubmodel'+arch
        with open(file_path,'wb') as fout:
            pickle.dump(self.estimator,fout)
            pickle.dump(self.hub_name,fout)
            pickle.dump(self.text_transformer, fout)
            pickle.dump(self.title_transformer, fout)

    def load(self, file_path):
        "Load model data from file"
        with open(file_path,'rb') as fin:
            self.estimator = pickle.load(fin)
            self.hub_name = pickle.load(fin)
            self.text_transformer = pickle.load(fin)
            self.title_transformer = pickle.load(fin)

def load_model(file_path):
    """
    Load model from file and return
        :param file_path: path to file with model data
    """
    model = HabrHubRatingRegressor('')
    model.load(file_path)
    return model

def model_from_db(hub_name, text_db_path, start_index=1, operations=4):
    """
    Make model from file with text parsed posts data 
        :param hub_name: name of target hub
        :param text_db_path: path to file with text parsed posts data
        :param start_index: start index for progress message
        :param operations: count of all operations in progress messages
    """
    vec_db_path = f"vec_{hub_name}.pickle"
    space_db_path = f"space_{hub_name}.pickle"
    db.cvt_text_db_to_vec_db(text_db_path, vec_db_path, space_db_path, start_index=start_index, operations=6)
    space_text, space_title = db.load_hub_vectorizers(space_db_path)
    print(f'[{start_index+2}/{operations}]')
    X, y = db.cvt_db_to_DataFrames(vec_db_path)
    X, y = shuffle(X,y)
    hub = HabrHubRatingRegressor(hub_name)
    print(f'[{start_index+3}/{operations}]')
    hub.fit(X,y)
    hub.set_transformers(space_text, space_title)
    return hub

def make_and_save_model_from_db(hub_name, text_db_path):
    """
    Create mode from db and save with default path
        :param hub_name: name of target hub
        :param text_db_path: path to text db
    """
    hub = model_from_db(hub_name,text_db_path)
    hub.save()

def model_from_hub(hub_name, threads_count=16):
    """
    Create model from hub
        :param hub_name: name of target hub
        :param threads_count: count of loaded threads
    """
    text_db_path = f"{hub_name}.pickle"
    parser.db.save_hub_to_db(hub_name, text_db_path, start_index=1, operations=6)
    return model_from_db(hub_name, text_db_path, start_index=3, operations=6)

def make_and_save_model_from_hub(hub_name):
    """
    Create model from hub and save with default path
        :param hub_name: name of target hub
    """
    hub = model_from_hub(hub_name)
    hub.save()
