from gensim import models
import redis
import re
import sys

model = models.Word2Vec.load_word2vec_format(sys.argv[1], binary=True)
print "model loaded OK"

def tokenize(rawString):
    s = ''.join(sym for sym in rawString.lower() if ord(sym)<128)
    tokens = map(lambda token: token.strip(), re.sub("[^A-Za-z0-9']+", ' ', s).replace("'s", '').split(" "))
    return filter(lambda token: len(token) != 0, tokens)

def parseTweet(tweet):
    colonPos = tweet.index(":")
    return (tweet[:colonPos], tokenize(tweet[colonPos+1:]))

def getSynonyms(word, nsyms = 10, depth = 30000):
    if word not in model.vocab:
        print "No such word " + word
        return []
    print "Adding word " + word
    return model.most_similar(word)

def addSynonymsToRedis(word, syns):
    for syn in syns:
        redis.zadd(word, syn[1], syn[0])

redis = redis.StrictRedis(sys.argv[3], port=6379, db=0)
first = model.index2word[32070:int(sys.argv[2])]
i = 0
for word in first:
        i = i + 1
        word = ''.join(sym for sym in word if ord(sym)<128)
        lower = word.lower()
        if len(lower) == 0 or word not in model.vocab:
                continue
        if  lower != word and lower not in model.vocab:
                addSynonymsToRedis(lower, model.most_similar(word))
        else:
                addSynonymsToRedis(word, model.most_similar(word))

        print word
        print i
