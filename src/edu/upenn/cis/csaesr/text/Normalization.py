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
from text.NormalizationDictionaries import example_sents, singles, ones, teens, tens, places, ones_tens_dict

class Normalize(object):
    """Normalization should be applied equally to all transcriptions."""

    def __init__(self):
        self.sent_procs = {"strip_whitespace": self.strip_whitespace}
        self.word_procs = {"to_lower" : self.to_lower,
                          "from_hyphen" : self.from_hyphen,
                          "from_numeric" : self.from_numeric
                          }
        
    
    def strip_whitespace(self,sent):
        return sent.lstrip().strip()
    
    def to_lower(self,word):
        return [word.lower()]

    def from_hyphen(self,word):
        return word.split("-")
    
    def proc_dict(self,sent_procs,word_procs,hyp,ref=None):
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
        result = []
        word =  self.ones_tens(ones_tens_dict, numeric_word)
        remainder, word = self.base_tup(word)
        if not remainder:
            return word
        if word:
            result.append(word)
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
    #     normalizer.proc_dict(normalizer.sent_procs,normalizer.word_procs,example_sents[sent]["hyp"],example_sents[sent]["ref"])
    #===========================================================================
    #print(normalizer.from_numeric("216"))
    #print(normalizer.from_numeric("6"))
    #print(normalizer.from_numeric("20"))
    #print(normalizer.from_numeric("620th"))
    print(normalizer.from_numeric("621st"))
    print(normalizer.from_numeric("11"))
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
        
    
if __name__=="__main__":
    main()