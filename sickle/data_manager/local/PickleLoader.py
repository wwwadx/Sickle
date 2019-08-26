import pickle
import os
import re
from tqdm import tqdm


class PickleLoader:
    def __init__(self, path):
        """
        init
        Args:
            path: 存取pickle文件的绝对路径
        """
        self.path = path
        if not os.path.exists(path):
            os.mkdir(path)

    def save_pickle(self, target, name, given_path=None):
        """
        缓存pickle文件
        Args:
            target: 要缓存的目标变量
            name: 缓存的文件名， str
            given_path: 如果指定路径则使用该路径，不指定用类默认路径

        Returns:

        """
        if given_path:
            path_info = '{0}/{1}.pkl'.format(given_path, name)
        else:
            path_info = '{0}/{1}.pkl'.format(self.path, name)
        with open(path_info, 'wb') as f:
            pickle.dump(target, f)

    def read_pickle(self, name, given_path=None):
        """
        读取缓存pickle的文件
        Args:
            name: 缓存的文件名， str
            given_path: 如果指定路径则使用该路径，不指定用类默认路径

        Returns:

        """
        if given_path:
            path_info = '{0}/{1}.pkl'.format(given_path, name)
        else:
            path_info = '{0}/{1}.pkl'.format(self.path, name)
        if not os.path.exists(path_info):
            print('No such file, ', path_info)
            res = None
        else:
            with open(path_info, 'rb') as f:
                res = pickle.load(f)
        return res

    def pickle_exists(self, name, given_path=None):
        """
        判断对应缓存是否存在
        Args:
            name: pickle文件的名字，不包含后缀
            given_path: 如果指定路径则使用该路径，不指定用类默认路径

        Returns: 存在True 不存在False

        """
        if given_path:
            path_info = '{0}/{1}.pkl'.format(given_path, name)
        else:
            path_info = '{0}/{1}.pkl'.format(self.path, name)
        return os.path.exists(path_info)

    def all_cache(self, given_path=None):
        """
        对应路径下全部缓存的文件名
        Args:
            given_path: 如果指定路径则使用该路径，不指定用类默认路径

        Returns:全部文件名

        """
        if given_path:
            return [re.findall(r'(.+?)\.', i)[0] for i in os.listdir(given_path) if i.split('.')[-1] == 'pkl']
        else:
            return [re.findall(r'(.+?)\.', i)[0] for i in os.listdir(self.path) if i.split('.')[-1] == 'pkl']

    def del_cache(self, name, given_path=None):
        """
        删除指定文件的缓存
        Args:
            name: 文件名
            given_path: 如果指定路径则使用该路径，不指定用类默认路径

        Returns:

        """
        if given_path:
            os.remove('{0}/{1}.pkl'.format(given_path, name))
        else:
            os.remove('{0}/{1}.pkl'.format(self.path, name))

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


