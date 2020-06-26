import unittest
from test_method import TestMethod
from test_extractor import TestExtractor
from test_P import TestP

'''
A script to run all the test cases.
'''
# load test suites
method_suite = unittest.TestLoader().loadTestsFromTestCase(TestMethod)
extractor_suite = unittest.TestLoader().loadTestsFromTestCase(TestExtractor)
P_suite = unittest.TestLoader().loadTestsFromTestCase(TestP)
# combine the test suites
suites = unittest.TestSuite([method_suite,
                             extractor_suite,
                             P_suite])
# run the test suites
unittest.TextTestRunner(verbosity=2).run(suites)
