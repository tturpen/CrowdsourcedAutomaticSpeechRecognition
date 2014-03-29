"""This tests the modules in this directory"""
from filtering.StandardFilter import Filter
import unittest


class Test(unittest.TestCase):    
    def setUp(self):
        self.filter = Filter()
        
    def test_filter_cat(self):
        ref = "This is a cattest test.".split()
        hyp = "This is a cat test test.".split()
        self.assertEqual(self.filter.cat(ref,hyp), ref,"Filter rejected hyp using cat.")
        
    def test_normalization(self):
        ref = [ "DO" , "THE" , "CASREP", "GUARDFISH+S" ,"BCD13","4379th", "ALERTS" , "INCLUDE" , "ICE-NINE"]
        hyp = ["do", "the", "cas", "rep", "guardfish's", "BCD","thirteen","four","thousand","three","hundred","seventy","ninth","alerts", "include", "ice", "nine"]
        self.assert_(self.filter.approve_transcription(" ".join(ref)," ".join(hyp))[0])
        
    def test_backwards_compatible_normalization(self):
        ref = [ "DO" , "THE" , "CASREP", "GUARDFISH+S" ,"BCD13","4379th", "ALERTS" , "INCLUDE" , "ICE-NINE"]
        new_ref = self.filter.get_normalized_list(" ".join(ref))
        ref = self.filter.get_normalized_list(" ".join(new_ref))
        self.assertEquals(self.filter.approve_transcription(" ".join(new_ref)," ".join(ref))[1],0,"Backwards normalization failed.")       
    
if __name__ == "__main__":
    unittest.main()