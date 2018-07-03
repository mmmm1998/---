from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error
from habrating import db, model

hub_name = 'webdev'
trees_count = 10

X,y = db.cvt_db_to_DataFrames(f'vec_{hub_name}.pickle')

X_train, X_test, y_train, y_test = train_test_split(X,y,test_size=0.3)

hub = model.HabrHubRatingRegressor(f'{trees_count}{hub_name}')
hub.estimator.n_estimators = trees_count
hub.text_transformer, hub.title_transformer = db.load_hub_vectorizers(f'space_{hub_name}.pickle')

hub.fit(X_train,y_train)

print(f'абсолютная ошибка на тренеровочной = {mean_absolute_error(y_train,hub.predict(X_train))}')
print(f'абсолютная ошибка на тесте = {mean_absolute_error(y_test,hub.predict(X_test))}')

hub.save()