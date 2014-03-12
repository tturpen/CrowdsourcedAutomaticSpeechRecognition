"""Normalize text using dictionaries and rules."""
# Copyright (C) 2014 Taylor Turpen
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
from text.NormalizationDictionaries import example_sents, singles, ones, teens, tens, places, ones_tens_dict, suffixes
from text.Segmentation import WordSegmenter
from collections import OrderedDict


class Normalize(object):
    """Normalization should be applied equally to all transcriptions."""

    def __init__(self):
        self.sent_procs = {"strip_whitespace": self.strip_whitespace}
        self.word_procs = OrderedDict([("to_lower" , self.to_lower),
                                       ("split_abbrev", self.split_abbreviations),
                                       ("split_apostrophe", self.from_apostrophe),                                       
                          ("from_hyphen" , self.from_hyphen),
                          ("from_numeric" , self.from_numeric),
                          ])
        self.segementer = WordSegmenter("en")
        
    def split_abbreviations(self,word):
        """Split alphanumeric words"""
        result = []
        if not (word.isalnum() and not word.isdigit() and not word.isalpha())\
            or any([word.endswith(suffix) for suffix in suffixes]):
            #If not letters and digits
            return [word]
        while True:
            #word contains letters and numbers, split left from right            
            starts_alpha = word[0].isalpha()            
            for i,c in enumerate(word):
                if not c.isalpha() and starts_alpha:
                    result.append(word[:i])
                    word = word[i:]
                    break
                elif c.isalpha() and not starts_alpha:
                    result.append(word[:i])
                    word = word[i:]
                    break            
            if not (word.isalnum() and not word.isdigit() and not word.isalpha()):
                result.append(word)
                break
        return result
        
    def strip_whitespace(self,sent):
        return sent.lstrip().strip()
    
    def to_lower(self,word):
        return [word.lower()]

    def from_hyphen(self,word):
        return word.split("-")
    
    def from_apostrophe(self,word):
        return [word.replace("+s","'s")]
    
    def compare_sents(self,sent_procs,word_procs,hyp,ref=None):
        for func in sent_procs:
            hyp = sent_procs[func](hyp)
            ref = sent_procs[func](ref)
            
        for func in word_procs:            
            result = []
            for word in hyp:
                result.extend(word_procs[func](word))
            hyp = result
        if ref:
            print(result==ref)
        return result    
    
    def standard_normalization(self,sent):
        sent_procs = self.sent_procs
        word_procs = self.word_procs
        for func in sent_procs:
            sent = sent_procs[func](sent)
            
        sent = self.segementer.get_word_list(sent)
        for func in word_procs:            
            result = []
            for word in sent:
                result.extend(word_procs[func](word))
            sent = result
        return sent    
        
    
    def ones_tens(self,d,word):
        found = False
        for key in d:
            if word.endswith(key):
                if type(d[key]) == dict:
                    return (word[:-len(key)],self.ones_tens(d[key],word[:-len(key)]))
                else:
                    return (word[:-len(key)],d[key])
                found = True
        if not found:
            return d["OTHER"]
        return ""
    
    def base_tup(self,tups):
        if type(tups[-1]) == str:
            return tups
        else:
            return self.base_tup(tups[-1])
            
    def from_numeric(self,numeric_word):
        if numeric_word.isalpha() or \
            not any([numeric_word.endswith(suffix) for suffix in suffixes]) and\
            not numeric_word.isdigit():
            return [numeric_word]
        result = []
        word =  self.ones_tens(ones_tens_dict, numeric_word)
        if not word:
            return [numeric_word]
        remainder, word = self.base_tup(word)
        if word:
            result.append(word)
        if not remainder:
            return [word]

        place = ""
        for one in ones:
            if word in one:
                place = "ones" 
        #If not ones then word is tens, so remainder is hundreds
        if place == "ones":
            for i, ten in enumerate(tens):
                if i > 1 and remainder[-1] == str(i):
                    result.append(ten)
                    remainder = remainder[:-1]
                    break
            else:
                #zero tens digit
                remainder = remainder[:-1]
        for place in places:
            if remainder and remainder[-1] in singles:
                next = singles[remainder[-1]]
                if not word and numeric_word.endswith("th"):            
                    result.append(place+"th")
                else:
                    result.append(place)
                result.append(next)
                remainder = remainder[:-1]
            elif remainder:
                #zero digit
                remainder = remainder[:-1]
                
        return result[::-1]    
    
def main():
    #===========================================================================
    normalizer = Normalize()
    # for sent in example_sents:
    #     normalizer.compare_sents(normalizer.sent_procs,normalizer.word_procs,example_sents[sent]["hyp"],example_sents[sent]["ref"])
    #===========================================================================
    #print(normalizer.from_numeric("216"))
    #print(normalizer.from_numeric("6"))
    #print(normalizer.from_numeric("20"))
    #print(normalizer.from_numeric("620th"))
    print(normalizer.from_numeric("621st"))
    print(normalizer.from_numeric("11"))
    print(normalizer.from_numeric("13"))
    print(normalizer.from_numeric("11th"))
    print(normalizer.from_numeric("3rd"))
    print(normalizer.from_numeric("611th"))
    print(normalizer.from_numeric("611"))
    print(normalizer.from_numeric("21"))
    print(normalizer.from_numeric("20"))
    print(normalizer.from_numeric("2342nd"))
    print(normalizer.from_numeric("600"))
    print(normalizer.from_numeric("4600th"))
    print(normalizer.from_numeric("1112th"))
    print(normalizer.split_abbreviations("ABC1234ABC1")== ["ABC","1234","ABC","1"]) 
        
    
if __name__=="__main__":
    main()