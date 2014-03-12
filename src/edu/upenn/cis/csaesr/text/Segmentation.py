'''
Created on Mar 12, 2014

@author: taylor
'''

class WordSegmenter(object):
    """Segement a sentence into a list of words."""
    def __init__(self,lang):
        self.lang = lang

    def get_word_list(self,sentence):
        if self.lang == "en":
            return sentence.split(" ")