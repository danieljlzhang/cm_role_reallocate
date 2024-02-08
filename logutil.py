import logging

def logmodule(logname):
    logger = logging.getLogger(logname)
    logger.setLevel(logging.INFO)
    # 文件处理
    handler = logging.FileHandler(logname, encoding='utf-8')
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%Y/%m/%d %H:%M:%S')
    handler.setFormatter(formatter)
    # 流处理
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console_format = logging.Formatter('%(asctime)s - %(module)s - %(lineno)d  %(levelname)s : %(message)s')
    console.setFormatter(console_format)

    logger.addHandler(handler)
    logger.addHandler(console)

    return logger