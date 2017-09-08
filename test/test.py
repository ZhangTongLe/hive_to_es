import sys


def get_map(param_list):
    """
    解析键值对形式的参数数组，返回dict
    :param param_list: 参数数组，如sys.argv
    :return:
    """

    param_dict = {}

    for pair in param_list:
        ls = pair.split('=')
        param_dict[ls[0]] = ls[1]

    return param_dict


print(get_map(sys.argv[1:]))
