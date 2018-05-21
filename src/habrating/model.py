import pickle
import asyncio
from sklearn.ensemble import RandomForestRegressor
from sklearn.utils import shuffle

from . import db, parser

class HabrHubRatingRegressor:
    def __init__(self, hub_name):
        self.estimator = RandomForestRegressor(n_estimators = 100, n_jobs=-1, verbose=2)
        self.hub_name = hub_name
        self.text_transformer = None
        self.title_transformer = None

    def fit(self, X_train, y_train):
        self.estimator.fit(X_train, y_train)

    def predict(self, X):
        return self.estimator.predict(X)

    def predict_by_urls(self, urls):
        loop = asyncio.get_event_loop()
        posts = list(map(lambda url: loop.run_until_complete(parser.parse_article(url)), urls))
        return self.predict_by_posts(posts)

    def predict_by_posts(self, posts):
        for post in posts:
            db.vectorize_post(post, self.text_transformer, self.title_transformer)
        X, _ = db.cvt_to_DataFrames(posts)
        y_predict = self.predict(X)
        return y_predict

    def set_transformers(self, text_transformer, title_transformer):
        self.text_transformer = text_transformer
        self.title_transformer = title_transformer

    def save_to(self, file_path = None):
        if file_path is None:
            file_path = self.hub_name+'.hubmodel'
        with open(file_path,'wb') as fout:
            pickle.dump(self.estimator,fout)
            pickle.dump(self.hub_name,fout)
            pickle.dump(self.text_transformer, fout)
            pickle.dump(self.title_transformer, fout)

    def load_from(self, file_path = None):
        if file_path is None:
           file_path = self.hub_name+'.hubmodel'
        with open(file_path,'rb') as fin:
            self.estimator = pickle.load(fin)
            self.hub_name = pickle.load(fin)
            self.text_transformer = pickle.load(fin)
            self.title_transformer = pickle.load(fin)

def load_model(file_path):
    model = HabrHubRatingRegressor('')
    model.load_from(file_path)
    return model

def model_from_hub(hub_name, threads_count=16):
	text_db_path = f"{hub_name}.pickle"
	db.save_hub_to_db(hub_name, text_db_path, start_index=1, operations=7, threads_count=threads_count)
	vec_db_path = f"vec_{hub_name}.pickle"
	space_db_path = f"space_{hub_name}.pickle"
	db.cvt_text_db_to_vec_db(text_db_path, vec_db_path, space_db_path, start_index=4, operations=7)
	space_text, space_title = db.load_words_space(space_db_path)
	print('[6/7]')
	X, y = db.cvt_db_to_DataFrames(vec_db_path)
	X, y = shuffle(X,y)
	hub = HabrHubRatingRegressor(hub_name)
	print('[7/7]')
	hub.fit(X,y)
	hub.set_transformers(space_text, space_title)
	return hub

def make_and_save_model_from_hub(hub_name):
	hub = model_from_hub(hub_name)
	hub.save_to()
