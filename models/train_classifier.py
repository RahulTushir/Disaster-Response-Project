import sys
import sqlite3
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import pickle

import nltk
nltk.download(['punkt', 'wordnet', 'averaged_perceptron_tagger'])
from nltk.tokenize import word_tokenize
from nltk.stem import WordNetLemmatizer
from sklearn.pipeline import Pipeline
from sklearn.metrics import confusion_matrix
from sklearn.model_selection import GridSearchCV, train_test_split
from sklearn.feature_extraction.text import CountVectorizer, TfidfTransformer
from sklearn.multioutput import MultiOutputClassifier
from sklearn.multiclass import OneVsRestClassifier
from sklearn.svm import LinearSVC
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import f1_score, recall_score, classification_report

def load_data(database_filepath):    
    #engine = create_engine('sqlite:///' + database_filepath)
    engine=sqlite3.connect(database_filepath)
    #Related 2 is having 2 also as one of predicted values those rows are dropped.
    df = pd.read_sql('select * from myTableName where related!=2', engine) 
    X = df['message']
    Y = df.iloc[:,4:]
    category_names = list(df.columns[4:])
    return X, Y, category_names


def tokenize(text):
    text=text.lower()
    tokens=word_tokenize(text)
    
    lemmatizer = WordNetLemmatizer()
    clean_tokens = []
    for tok in tokens:
        clean_tok = lemmatizer.lemmatize(tok).lower().strip()
        clean_tokens.append(clean_tok)
    return clean_tokens


def build_model():
    pipeline = Pipeline([
                    ('message', CountVectorizer(tokenizer=tokenize)),
                    ('tfidf_t', TfidfTransformer()),
                    ('clf', MultiOutputClassifier(LinearSVC()))
                ])
    parameters = {'message__ngram_range': ((1, 1), (1, 2)),
                  'message__max_df': (0.5, 1.0)
                  }

    cv = GridSearchCV(estimator=pipeline,
            param_grid=parameters,
            verbose=3,
            cv=3)
    return cv


def evaluate_model(model, X_test, Y_test, category_names):
    y_pred=model.predict(X_test)
    print(classification_report(Y_test, y_pred,target_names=category_names))


def save_model(model, model_filepath):
    pickle.dump(model, open(model_filepath, 'wb'))    


def main():
    if len(sys.argv) == 3:
        database_filepath, model_filepath = sys.argv[1:]
        print('Loading data...\n    DATABASE: {}'.format(database_filepath))
        X, Y, category_names = load_data(database_filepath)
        Y.drop('child_alone', axis = 1, inplace = True)# only one value
        X_train, X_test, Y_train, Y_test = train_test_split(X, Y, test_size=0.2)

        print('Building model...')
        model = build_model()

        print('Training model...')
        model.fit(X_train, Y_train)

        print('Evaluating model...')
        evaluate_model(model, X_test, Y_test, category_names)

        print('Saving model...\n    MODEL: {}'.format(model_filepath))
        save_model(model, model_filepath)

        print('Trained model saved!')

    else:
        print('Please provide the filepath of the disaster messages database '\
              'as the first argument and the filepath of the pickle file to '\
              'save the model to as the second argument. \n\nExample: python '\
              'train_classifier.py ../data/DisasterResponse.db classifier.pkl')


if __name__ == '__main__': 
    main()
