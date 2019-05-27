from airflow.operators.sensors import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
import logging
import requests


class Flo2dCompletionSensor(BaseSensorOperator):
    @apply_defaults
    def __init__(self, input_params,*args, **kwargs):
        #self.ip_address = '192.168.1.39'
        #self.port = '8095'
        self.ip_address = input_params['ip_address']
        self.port = input_params['port']
        #logging.info("Flo2dCompletionSensor inputs: ", [self.ip_address, self.port])
        super(Flo2dCompletionSensor, self).__init__(*args, **kwargs)

    def poke(self, context):
        logging.info('-----------------------------------------------------------------------')
        execution_dt = context['execution_date']
        run_date = execution_dt.strftime('%Y-%m-%d')
        run_time = execution_dt.strftime('%H:00:00')
        payload = {'run_date': run_date, 'run_time': run_time}
        url = 'http://'+self.ip_address+':'+self.port+'/flo2d-completed'
        #logging.info("Flo2dCompletionSensor url: ", url)
        print(("Flo2dCompletionSensor payload: ", payload))
        print(("Flo2dCompletionSensor url: ", url))
        result = requests.get(url=url, params=payload)
        logging.info('check_file_status|result : ', result.json())
        rest_data = result.json()
        logging.info('-----------------------------------------------------------------------')
        return rest_data['completed']
