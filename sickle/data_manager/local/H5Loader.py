import pandas as pd
import os
import re
from tqdm import tqdm


class H5Loader:
    """
    针对DataFrame的H5格式存取类
    """
    def __init__(self, path):
        """
        init
        Args:
            path: 存取h5文件的绝对路径
        """
        self.path = path
        if not os.path.exists(path):
            os.mkdir(path)

    def cache(self, df, name, given_path=None):
        """
        缓存h5文件
        Args:
            df: 要缓存的DataFrame
            name: 缓存的文件名， str
            given_path: 如果指定路径则使用该路径，不指定用类默认路径

        Returns:

        """
        if given_path:
            path_info = '{0}/{1}.h5'.format(given_path, name)
        else:
            path_info = '{0}/{1}.h5'.format(self.path, name)
        h5 = pd.HDFStore(path_info, complevel=9, complib='blosc')
        h5['data'] = df
        h5.close()

    def load_cache(self, name, given_path=None):
        """
        读取h5文件
        Args:
            name: h5文件的名字，不包含h5
            given_path: 如果指定路径则使用该路径，不指定用类默认路径

        Returns: pd.DataFrame

        """
        if given_path:
            path_info = '{0}/{1}.h5'.format(given_path, name)
        else:
            path_info = '{0}/{1}.h5'.format(self.path, name)
        if self.cache_exist(name):
            store = pd.HDFStore(path_info)
            df = pd.read_hdf(store)
            store.close()
        else:
            df = None
        return df

    def cache_exist(self, name, given_path=None):
        """
        判断对应缓存是否存在
        Args:
            name: h5文件的名字，不包含h5
            given_path: 如果指定路径则使用该路径，不指定用类默认路径

        Returns: 存在True 不存在False

        """
        if given_path:
            path_info = '{0}/{1}.h5'.format(given_path, name)
        else:
            path_info = '{0}/{1}.h5'.format(self.path, name)
        return os.path.exists(path_info)

    def all_cache(self, given_path=None):
        """
        对应路径下全部缓存的文件名
        Args:
            given_path: 如果指定路径则使用该路径，不指定用类默认路径

        Returns:全部文件名

        """
        if given_path:
            return [re.findall(r'(.+?)\.', i)[0] for i in os.listdir(given_path)]
        else:
            return [re.findall(r'(.+?)\.', i)[0] for i in os.listdir(self.path)]

    def del_cache(self, name, given_path=None):
        """
        删除指定文件的缓存
        Args:
            name: 文件名
            given_path: 如果指定路径则使用该路径，不指定用类默认路径

        Returns:

        """
        if given_path:
            os.remove('{0}/{1}.h5'.format(given_path, name))
        else:
            os.remove('{0}/{1}.h5'.format(self.path, name))

    def del_all_cache(self, given_path=None):
        """
        删除指定路径全部缓存
        Args:
            given_path: 如果指定路径则使用该路径，不指定用类默认路径

        Returns:

        """
        if given_path:
            all_file = os.listdir(given_path)
            for files in tqdm(all_file):
                os.remove('{0}/{1}'.format(given_path, files))
        else:
            all_file = os.listdir(self.path)
            for files in tqdm(all_file):
                os.remove('{0}/{1}'.format(self.path, files))
