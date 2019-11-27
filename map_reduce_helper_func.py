import numpy as np
from functools import reduce
from collections import Counter
import re
import nltk
from multiprocessing import Pool
from nltk.corpus import stopwords
nltk.download('stopwords')

ENGLISH_STOP_WORDS = set(stopwords.words('english'))

def word_not_in_stopwords(word):
    return word not in ENGLISH_STOP_WORDS and word and word.isalpha()

def clean_word(word):
    return re.sub(r'[^\w\s]','',word).lower()

def chunkify(lst,n):
       return [lst[i::n] for i in range(n)]
    
def mapper(text):
    tokens_in_text = text.split()
    tokens_in_text = map(clean_word, tokens_in_text)
    tokens_in_text = filter(word_not_in_stopwords, tokens_in_text)
    return Counter(tokens_in_text)

def reducer(cnt1, cnt2):
    cnt1.update(cnt2)
    return cnt1

def chunk_mapper(chunk):
    mapped = map(mapper, chunk)
    reduced = reduce(reducer, mapped)
    return reduced
