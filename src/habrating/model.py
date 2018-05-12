import pickle
from sklearn.ensemble import RandomForestRegressor

class HabrHubRatingRegressor:
    def __init__(self, hub_name):
        self.estimator = RandomForestRegressor(n_estimators = 50, n_jobs=-1, verbose=2)
        self.hub_name = hub_name

    def fit(self, X_train, y_train):
        self.estimator.fit(X_train, y_train)

    def predict(self, X):
        return self.estimator.predict(X)

    def save_to(self, file_path):
        with open(file_path,'wb') as fout:
            pickle.dump(self.estimator,fout)
            pickle.dump(self.hub_name,fout)

    def load_from(self, file_path):
        with open(file_path,'rb') as fin:
            self.estimator = pickle.load(fin)
            self.hub_name = pickle.load(fin)
