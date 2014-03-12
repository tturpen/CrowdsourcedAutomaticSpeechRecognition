"""This tests the modules in this directory"""
from filtering.Standard import Filter
import unittest


class Test(unittest.TestCase):    
    def setUp(self):
        self.filter = Filter()
        
    def test_filter_cat(self):
        ref = "This is a cattest test.".split()
        hyp = "This is a cat test test.".split()
        self.assertEqual(self.filter.cat(ref,hyp), ref,"Filter rejected hyp using cat.")
        
    def test_normalization(self):
        ref = [ "DO" , "THE" , "CASREP", "GUARDFISH+S" ,"BCD13" "ALERTS" , "INCLUDE" , "ICE-NINE"]
        hyp = ["do", "the", "cas", "rep", "guardfish's", "BCD","thirteen","alerts", "include", "ice", "nine"]
        self.filter.approve
    
if __name__ == "__main__":
    unittest.main()