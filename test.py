import pickle


seen_file = "seen_articles.pkl"
loaded_deque = pickle.load(open(seen_file,'rb'))

print(loaded_deque)