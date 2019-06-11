import logging


def get_logger():
    # Create & configure logger
    #LOG_FORMAT = '%(levelname)s %(asctime)s - %(message)s'
    LOG_FORMAT = '[%%(asctime)s] {%%(filename)s:%%(lineno)d} %%(levelname)s - %%(message)s'
    logging.basicConfig(filename='/home/hasitha/PycharmProjects/Workflow/logs/workflow.log',
                        level=logging.DEBUG,
                        format=LOG_FORMAT)
    return logging.getLogger()

