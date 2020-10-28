import math
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
'''NLP Tools'''
import nltk
from nltk.corpus import wordnet as wn
from nltk.stem.wordnet import WordNetLemmatizer
from nltk.tokenize import TweetTokenizer
import string
import re
from nltk.corpus import stopwords
from textblob import TextBlob as tb
from tqdm import tqdm
from blob_modules import import_from_blob, export_to_blob

# !python -m spacy download en_core_web_sm

nltk.download('wordnet')
nltk.download('stopwords')
nltk.download('punkt')

def main():
    import_from_blob()
    text_mining('appended_output')


def text_mining(input_file):

    print("about to start producing tf-idf data")
    df = pd.read_csv(input_file,encoding='cp1252')
    df.incident_date = pd.to_datetime(df.incident_date)

    def clean_text(text):
        """Used to clean and prepare source data"""
        global cleaned_text
        # remove numbers
        text_nonum = re.sub(r'\d+', '', text)
        # remove punctuations and convert characters to lower case
        text_nopunct = "".join([char.lower() for char in text_nonum if char not in string.punctuation]) 
        # substitute multiple whitespace with single whitespace
        # Also, removes leading and trailing whitespaces
        text_no_doublespace = re.sub('\s+', ' ', text_nopunct).strip()
        #tokenise text
        tokenised_text = text_no_doublespace.split()
        for word in tokenised_text:
            if len(word) == 1:
                tokenised_text.remove(word)
            #if word is a stop word, remove it from the list
            elif word in stopwords.words('english'):
                tokenised_text.remove(word)
                #de-tokenise text
                cleaned_text = ' '.join(tokenised_text)
        return cleaned_text

    def tf(word, blob):
        """Determines relative term frequency"""
        return blob.words.count(word) / len(blob.words)

    def n_containing(word, bloblist):
        """Counts the number of occurances of the given word"""
        return sum(1 for blob in bloblist if word in blob.words)

    def idf(word, bloblist):
        """Determines inverse document frequency"""
        return math.log(len(bloblist) / (1 + n_containing(word, bloblist)))

    def tfidf(word, blob, bloblist):
        """Creates tf-idf score using tf and idf functions"""
        return tf(word, blob) * idf(word, bloblist)

    def df_to_text(df=df, col='cleaned_t'):
        """Function to take in df row, pass it through tb() and store it in bloblist."""
        df['tb_col'] = df[col].apply(tb)
        bloblist = df['tb_col'].to_list()
        return bloblist

    #replace nan values with empty string 
    df['narrative'] = df['narrative'].replace(np.nan,"",regex=True)
    #apply above func on the narrative text
    df['cleaned_t'] = df['narrative'].apply(clean_text)

    #specify the number of words to extract
    TOP_WORD_COUNT=5

    bloblist = df_to_text()

    #create an empty list to append top words
    all_top_words = []

    #loop to extract tf-idf output
    for i, blob in tqdm(enumerate(bloblist)):
        #outputs a dict of word:tf-idf score
        scores = {word: tfidf(word, blob, bloblist) for word in blob.words}
        # list of tuples containing (word:tf-idf-score) sorted by tf-idf-score
        sorted_words = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        top_words = []
        #get 3 words with highest tf-idf-score
        for word, score in sorted_words[:TOP_WORD_COUNT]:
            top_words.append(word)
        top_words = [', '.join(top_words)]
        all_top_words.append(top_words)
    
    #change list to numpy array
    all_top_words = np.array(all_top_words)
    #append numpy array as df column
    df['top_words'] = all_top_words

    # save to csv
    df.to_csv('appended_output\appended_data_with_text_mining.csv',encoding='cp1252')


if __name__ == '__main__':
    main()