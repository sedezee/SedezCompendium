import unittest
import sedezcompendium.common.SQLObjects as SQLObjects

class TestRowMethods(unittest.TestCase): 
    class nRow(SQLObjects.Row): 
        __columns__ = ()
        def __init__(self, *rows, backwards = False):
            c = []
            self.__columns__ = ()
            for i in range(1, len(rows) + 1): 
                c.append(str(i))
            
            if backwards: 
                c.reverse()

            self.__columns__ = tuple(c)
            super().__init__(*rows)
    
    class NTable(SQLObjects.Table):
        __rows__ = []
        __columns__ = ()

        def __init__(self, *args):
            self.__rows__ = [] 
            self.__columns__ = ()
            super().__init__(*args)

    def test_row_equals(self):
        self.assertEqual(self.nRow(1), self.nRow(1))
        self.assertNotEqual(self.nRow(1), self.nRow(2))
        self.assertEqual(self.nRow(1, 4), self.nRow(1, 4))
        self.assertNotEqual(self.nRow(1, 4), self.nRow(1, 4, backwards = True))
        self.assertNotEqual(self.nRow(1, 4), self.nRow(4, 1))
        self.assertNotEqual(self.nRow(1, 4), self.nRow(1, 4, 5))
        self.assertNotEqual(self.nRow(1, 4), self.nRow(8, 2, 3))        
        with self.assertRaises(NotImplementedError): 
            self.assertEqual(self.nRow(1), 1)
        
    def test_row_table_equals(self): 
        self.assertEqual(self.nRow(1), self.NTable(self.nRow(1)))
        self.assertNotEqual(self.nRow(1), self.NTable(self.nRow(1), self.nRow(1)))
        self.assertNotEqual(self.nRow(2), self.NTable(self.nRow(1)))
        
    def test_table_equals(self): 
        default_oneRow = self.nRow(1)
        default_twoRow = self.nRow(1, 2)
        backwards_twoRow = self.nRow(1, 2, backwards = True)
        self.assertEqual(self.NTable(self.nRow(1)), self.NTable(self.nRow(1)))
        self.assertEqual(self.NTable(self.nRow(1, 2)), self.NTable(self.nRow(1, 2)))
        self.assertNotEqual(self.NTable(default_oneRow), self.NTable(default_twoRow))
        self.assertNotEqual(self.NTable(default_twoRow), self.NTable(backwards_twoRow))
        self.assertEqual(self.NTable(default_oneRow, default_oneRow), self.NTable(default_oneRow, default_oneRow))
        self.assertNotEqual(self.NTable(default_oneRow), self.NTable(default_oneRow, default_oneRow))
        with self.assertRaises(NotImplementedError): 
            self.assertEqual(self.NTable(default_oneRow), 1)
    
    def test_table_row_equals(self): 
        self.assertEqual(self.NTable(self.nRow(1)), self.nRow(1))
        self.assertNotEqual(self.NTable(self.nRow(1), self.nRow(1)), self.nRow(1))
        self.assertNotEqual(self.NTable(self.nRow(1)), self.nRow(2))

    def test_row_in_table(self): 
        self.assertIn(self.nRow(1), self.NTable(self.nRow(1)))
        self.assertNotIn(self.nRow(1), self.NTable(self.nRow(2)))
        self.assertIn(self.nRow(1), self.NTable(self.nRow(2), self.nRow(1)))


if __name__ == '__main__':
    unittest.main()