from pycloudmessenger.ffl import fflapi
from pycloudmessenger.ffl import abstractions as fflabc


class COSModelDB:

    def __init__(self, credentials:dict, broker_context:dict) -> None:
        self.credentials = credentials
        self.reg_url = credentials.get('register_url', None)
        self.reg_api_key = credentials.get('register_api_key', None)
        self.messenger = fflapi.Messenger(broker_context)

    def initialize_context(self, username, password):
        fflapi.create_user(username, password, 'ibm', url=self.reg_url, api_key=self.reg_api_key)
        self.context = fflabc.Factory.context('cloud', self.credentials, username, password, dispatch_threshold = 0)
        self.cos_user = fflabc.Factory.user(self.context)
    
    def save_model(self, model_key:str, model) -> dict:
        """Save a model and determine its download location.

        Args:
            model_key (str): Unique model key
            model: ML model to be sent

        Returns:
            dict: download location information
        """        

        response = self.messenger._dispatch_model(task_name=model_key, model=model)
        return response
    
    def load_model(self, model_key: str):
        """Returns a list with all the available trained models.

        Args:
            model_key (str): Unique model key

        Returns:
            model: The available model against the model_key
        """        
        return self.messenger.model_info(model_key)