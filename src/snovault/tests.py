import os
from tempfile import TemporaryDirectory
from unittest import TestCase
import numpy as np
import pandas as pd
from pset_3.data import *
from pset_utils.parquet import write_df_to_parquet


class DataTests(TestCase):

    def test_load_words(self):
        with TemporaryDirectory() as tmp:
            fp = os.path.join(tmp, 'test_words.txt')
            test_words = ['word1', 'word2', 'word3']
            with open(fp, 'w') as f:
                f.write('\n'.join(test_words))
            found_words = load_words(fp)
            assert found_words == test_words

    def test_load_vectors(self):
        with TemporaryDirectory() as tmp:
            fp = os.path.join(tmp, 'test_vectors.npy')
            test_vec = np.array([1,2,3,5])
            np.save(fp, test_vec)
            found_vec = load_vectors(fp)
            assert np.array_equal(found_vec, test_vec)

    def test_load_data(self):
        with TemporaryDirectory() as tmp:
            test_df = pd.DataFrame(data={'col': [1, 2]})
            df_name = os.path.join(tmp, 'test_parquet')
            target = write_df_to_parquet(test_df, df_name)
            assert os.path.exists(target)
            read_df = pd.read_parquet(target, columns=['col'])
            assert read_df.to_dict() == test_df.to_dict()

    def test_cosine_similarity(self):
        v1 = np.array([1,1,1])
        v2 = np.array([0,0,0])
        res1 = cosine_similarity(v1, v2)
        assert np.isnan(res1)
        # test value from Wolfram Alpha
        v3 = np.array([2,3,4])
        res2 = cosine_similarity(v1, v3)
        assert res2.round(6) == 0.964901
