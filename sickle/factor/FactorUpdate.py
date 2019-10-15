import Sickle.sickle.factor.factor_def.basic as f_basic
import Sickle.sickle.factor.factor_def.original as f_original
import Sickle.sickle.factor.factor_def.trend as f_trend
from Sickle.sickle.factor.factor_def import *
from tqdm import tqdm
import multiprocessing


def run_fac(factor, frequence, n=None, trend=None):
    if trend is None and n is None:
        fac = eval(factor)(frequence)
        fac.update()
    if trend is None and n is not None:
        fac = eval(factor)(frequence, n)
        fac.update()
    if trend is not None and n is not None:
        fac = eval(factor)(frequence, trend, n)
        fac.update()


def update_basic(frequency_list):
    all_factors = f_basic.__all__
    pool = multiprocessing.Pool(processes=multiprocessing.cpu_count())
    for factor in tqdm(all_factors):
        if factor.startswith('ORIGINAL'):
            for frequence in frequency_list:
                pool.apply_async(run_fac, args=(factor, frequence, ))
        # if factor.startswith('TREND'):
        #     for frequence in frequency_list:
        #         for n in [2, 4, 6, 8, 10, 20, 40, 60]:
        #             pool.apply_async(run_fac, args=(factor, frequence, n, ))
    pool.close()
    pool.join()


def update_original(frequency_list, n_list):
    all_factors = f_original.__all__
    pool = multiprocessing.Pool(processes=multiprocessing.cpu_count())
    for factor in tqdm(all_factors):
        for frequence in frequency_list:
            for n in n_list:
                pool.apply_async(run_fac, args=(factor, frequence, n, ))
    pool.close()
    pool.join()


def update_trend(frequency_list, trend_list, n_list):
    all_factors = f_trend.__all__
    pool = multiprocessing.Pool(processes=multiprocessing.cpu_count())
    for factor in tqdm(all_factors):
        for frequence in frequency_list:
            for trend in trend_list:
                for n in n_list:
                    pool.apply_async(run_fac, args=(factor, frequence, n, trend, ))
    pool.close()
    pool.join()


def update_all():
    frequency_list = ['5min']
    trend_list = [2, 4, 6, 8, 10, 20, 40, 60]
    n_list = [5, 10, 15, 20, 40, 60]
    update_basic(frequency_list)
    update_original(frequency_list, n_list)
    update_trend(frequency_list, trend_list, n_list)


if __name__ == '__main__':
    # update_all()
    for fre in ['30min', '5min', '10min', '15min', '3min', 'half_day',
                '3min_minus_15s', '2min_45s', '4min', '6min']:
        update_basic([fre])
