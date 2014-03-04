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
from text.NormalizationDictionaries import example_sents, numeric_dict
d ={    "th":{    "4" : {"1": "fourteenth",
               "OTHER" : "forth"},
        "5" : {"1" : "fifteenth",
               "OTHER" :"fifth"},
        "6" : {"1" : "sixteenth",
               "OTHER" : "sixth"},
        "7" : {"1" : "seventeenth",
               "OTHER" : "seventh"},
        "8" : {"1" : "eighteenth",
               "OTHER" : "eighth"},
        "9" : {"1" : "nineteenth",
               "OTHER" : "ninth"},
        "2" : {"1": "twelfth"},
        "1" : {"1": "eleventh"},
        "0" : {"2":"twentieth"}},
    "nd": {"2": "second"},
    "rd": {"3" : "third"},
    "st": {"1" : "first"},
     "1" : {"1" : "eleven",
        "OTHER" : "one"},
     "2" : {"1" : "twelve",
        "OTHER" : "two"},
     "3" : {"1" : "thirteen",
        "OTHER" : "three"},
     "4" : {"1" : "forteen",
        "OTHER" : "four"},
     "5" : {"1" : "fifteen",
        "OTHER" : "five"},
     "6" : {"1" : "sixteen",
        "OTHER" : "six"},
     "7" : {"1" : "seventeen",
        "OTHER" : "seven"},
     "8" : {"1" : "eighteen",
        "OTHER" : "eight"},
     "9" : {"1" : "nineteen",
        "OTHER" : "nine"},
     "0" :{    "2" : "twenty",
        "3" : "thirty",
        "4" : "forty",
                          "5" : "fifty",
                          "6" : "sixty" ,
                          "7" : "seventy",
                          "8" : "eighty",
                          "9" : "ninety",
                          "1" : "ten"}}
class Normalize(object):
    """Normalization should be applied equally to all transcriptions."""

    def __init__(self):
        self.all_procs = {"to_lower" : self.to_lower,
                          "from_hyphen" : self.from_hyphen,
                          "from_numeric" : self.from_numeric}
    
    def to_lower(self,word):
        return [word.lower()]

    def from_hyphen(self,word):
        return word.split("-")
    
    def proc_dict(self,process_dict,orig,correct=None):
        for func in process_dict:
            result = []
            for word in orig:
                result.extend(process_dict[func](word))
            orig = result
        if correct:
            print(result==correct)
        return result    
    
    def from_numeric(self,word):
        result = []
        for key in d:
            if word.endswith(key):                
                i = -len(key)-1
                if len(word) >= abs(i) and word[i] in d[key]:
                    if type(d[key][word[i]]) == dict:
                        #th, nd rd
                        if word[i-1] in d[key][word[i]]:
                            return (word[:i-1],d[key][word[i]][word[i-1]],"")                        
                        else:
                            return (word[:i-1],d[key][word[i]]["OTHER"],"")                    
                    else:
                        #tens: sixteen, ten, eleven etc.
                        if word[i] in d[key]:
                            return (word[:i],d[key][word[i]],"")                   
                        else:
                            #normal ones
                            return (word[:i],"", d[key]["OTHER"])
                else:
                    #ones
                    return (word[:i],"", d[key]["OTHER"])    
        pass
    
def main():
    #===========================================================================
    normalizer = Normalize()
    # for sent in example_sents:
    #     normalizer.proc_dict(normalizer.all_procs,example_sents[sent]["orig"],example_sents[sent]["correct"])
    #===========================================================================
    #print(normalizer.from_numeric("216"))
    #print(normalizer.from_numeric("6"))
    #print(normalizer.from_numeric("20"))
    #print(normalizer.from_numeric("620th"))
    print(normalizer.from_numeric("621st"))
        
    
if __name__=="__main__":
    main()