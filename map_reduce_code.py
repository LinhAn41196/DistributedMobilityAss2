import map_reduce_helper_func as mp
from collections import Counter
import pandas as pd
data = pd.read_csv("Donald-Tweets!.csv").Tweet_Text

#Sequential 

def find_top_words(data):
    cnt = Counter()
    for text in data:
        tokens_in_text = text.split()
        tokens_in_text = map(mp.clean_word, tokens_in_text)
        tokens_in_text = filter(mp.word_not_in_stopwords, tokens_in_text)
        cnt.update(tokens_in_text)
        
    return cnt.most_common(10)

%time find_top_words(data)

#Parallelize

%time
data_chunks = map_reduce.chunkify(data, 2)
#step 1:
if __name__ ==  '__main__': 
    pool = Pool(2)
    mapped = pool.map(map_reduce.chunk_mapper, data_chunks)
    #step 2:
    reduced = reduce(map_reduce.reducer, mapped)
    print(reduced.most_common(10))