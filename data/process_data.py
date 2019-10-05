import sys
import sqlite3
import numpy as np
import pandas as pd
from sqlalchemy import create_engine

def load_data(messages_filepath, categories_filepath):
    # load categories dataset
    categories = pd.read_csv(categories_filepath,sep=',')
    # load messages dataset
    messages = pd.read_csv(messages_filepath, sep=',')

    df = pd.merge(messages, categories, how='outer', on='id')
    categories_col = df['categories'].str.split(';', n=36, expand=True)

    # select the first row of the categories dataframe
    row = categories_col.iloc[0]
    colName = row.replace('-.+', '', regex =True)

    # use this row to extract a list of new column names for categories.
    # one way is to apply a lambda function that takes everything .
    # up to the second to last character of each string with slicing
    category_colnames = np.array(colName)
    categories_col.columns = pd.Series(category_colnames)

    for column in categories_col:
        # set each value to be the last character of the string
        categories_col[column] = categories_col[column].str[-1]
        # convert column from string to numeric
        categories_col[column] = pd.to_numeric(categories_col[column])

    df.drop('categories', inplace=True, axis=1)

    # concatenate the original dataframe with the new `categories` dataframe
    df =pd.concat([df, categories_col], axis=1)

    return df


def clean_data(df):
    #drop duplicates in case if any ID field is getting repeated.
    df.drop_duplicates(subset='id', keep='first', inplace=True)
    #drop duplicates if any message is repeated.
    df.drop_duplicates(subset='message', keep='first', inplace=True)

    #drop bad rows of data other than 0 and 1 in predicted columns 
    

    return df


def save_data(df, database_filename):
    engine = create_engine('sqlite:///' + database_filename)
    df.to_sql('myTableName', engine, index=False)


def main():
    if len(sys.argv) == 4:

        messages_filepath, categories_filepath, database_filepath = sys.argv[1:]

        print('Loading data...\n    MESSAGES: {}\n    CATEGORIES: {}'
              .format(messages_filepath, categories_filepath))
        df = load_data(messages_filepath, categories_filepath)

        print('Cleaning data...')
        df = clean_data(df)

        print('Saving data...\n    DATABASE: {}'.format(database_filepath))
        save_data(df, database_filepath)

        print('Cleaned data saved to database!')

    else:
        print('Please provide the filepaths of the messages and categories '\
              'datasets as the first and second argument respectively, as '\
              'well as the filepath of the database to save the cleaned data '\
              'to as the third argument. \n\nExample: python process_data.py '\
              'disaster_messages.csv disaster_categories.csv '\
              'DisasterResponse.db')


if __name__ == '__main__':
    main()