"""
MIT License

Copyright (c) 2020 Airbyte

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple

import requests
import pprint

from base_python import AbstractSource, HttpStream, Stream
from base_python.cdk.streams.auth.core import NoAuth
from base_python.cdk.streams.auth.token import TokenAuthenticator
from requests.models import Response

# *********************************************************************************** 
# *********************************** TRANSFERWISE STREAM *************************
# ***********************************************************************************
class TransferwiseStream(HttpStream, ABC):
    # The url base. Required.
    url_base = 'https://api.sandbox.transferwise.tech/v1/'
    primary_key = ""


    def __init__(self, api_token: str, start_date: datetime, **kwargs):
        self.api_token = api_token
        self.start_date = start_date
        super().__init__()
    

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        """
        TODO: Override this method to define a pagination strategy. If you will not be using pagination, no action is required - just return None.

        This method should return a Mapping (e.g: dict) containing whatever information required to make paginated requests. This dict is passed
        to most other methods in this class to help you form headers, request bodies, query params, etc..

        For example, if the API accepts a 'page' parameter to determine which page of the result to return, and a response from the API contains a
        'page' number, then this method should probably return a dict {'page': response.json()['page'] + 1} to increment the page count by 1.
        The request_params method should then read the input next_page_token and set the 'page' param to next_page_token['page'].

        :param response: the most recent response from the API
        :return If there is another page in the result, a mapping (e.g: dict) containing information needed to query the next page in the response.
                If there are no more pages in the result, return None.
        """
        return None


    #def path(
    #    self, 
    #    stream_state: Mapping[str, Any] = None, 
    #    stream_slice: Mapping[str, Any] = None, 
    #    next_page_token: Mapping[str, Any] = None
    #) -> str:
    #    return "profiles"

    #@property
    #@abstractmethod
    #def personal_profile_id(self) -> str:
    #    pass
    

    def request_headers(
        self, **kwargs) -> Mapping[str, Any]:
        if self.api_token:
            return {'Authorization': f'Bearer {self.api_token}'}
        return {}


    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        The response is a simple JSON whose schema matches our stream's schema exactly, 
        The response is a simple JSON whose schema matches our stream's schema exactly,
        so we just return a list containing the response, an iterable containing each record in the response
        """ 
        return response.json()
    
    @property
    @abstractmethod
    def profilesID(self) -> Mapping[str, Any]:
        """The personal profile type of the field in the response which contains the data"""

    @property
    @abstractmethod
    def profilesID(self) -> Mapping[str, Any]:
        """The business profile type of the field in the response which contains the data"""
    

    def request_params(
            self,
            stream_state: Mapping[str, Any],
            stream_slice: Mapping[str, Any] = None,
            next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:
        # The api requires that we include the base currency as a query param so we do that in this method
        return {}

    def get_updated_state(
        self,
        current_stream_state: MutableMapping[str, Any],
        latest_record: Mapping[str, Any]
        ) -> Mapping[str, Any]:
        # This method is called once for each record returned from the API to compare the cursor field value in that record with the current state
        # we then return an updated state object. If this is the first time we run a sync or no state was passed, current_stream_state will be None.
        if current_stream_state is not None and 'date' in current_stream_state:
            current_parsed_date = datetime.strptime(current_stream_state['date'], '%Y-%m-%d')
            latest_record_date = datetime.strptime(latest_record['date'], '%Y-%m-%d')
            return {'date': max(current_parsed_date, latest_record_date).strftime('%Y-%m-%d')}
        else:
            return {'date': self.start_date.strftime('%Y-%m-%d')}
    

    def _chunk_date_range(self, start_date: datetime) -> List[Mapping[str, Any]]:
        """
        Returns a list of each day between the start date and now.
        The return value is a list of dicts {'date': date_string}.
        """
        dates = []
        while start_date < datetime.now():
            dates.append({'date': start_date.strftime('%Y-%m-%d')})
            start_date += timedelta(days=1)
        return dates

    
    def stream_slices(
        self,
        sync_mode,
        cursor_field: List[str] = None,
        stream_state: Mapping[str, Any] = None
        ) -> Iterable[Optional[Mapping[str, Any]]]:
        start_date = datetime.strptime(stream_state['date'], '%Y-%m-%d') if stream_state and 'date' in stream_state else self.start_date
        return self._chunk_date_range(start_date)


# *********************************************************************************** 
# *********************************** TRANSFERWISE PROFILES *************************
# ***********************************************************************************
class Profiles(TransferwiseStream):
    profilesID = dict()
    def __init__(self, api_token: str, start_date: datetime, **kwargs):
        super().__init__(api_token, start_date, **kwargs)

    
    #print("Main class: type_personal / type_business: ", self.type_personal, type_business)
    def path(self, **kwargs) -> str:
        return "profiles"
    
    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        for profile in response.json():
            if profile.get("type", None) == "personal":
                Profiles.profilesID['personal_id'] = profile.get("id", None)

            if profile.get("type", None) == "business":
                Profiles.profilesID['business_id'] = profile.get("id", None)
        #print("type_personal (ID) / type_business (ID): ", Profiles.profilesID['personal_id'], Profiles.profilesID['business_id'])
        return response.json()


# *********************************************************************************** 
# *********************************** CHECK PERSONAL ACCOUNT ************************
# ***********************************************************************************
class CheckPersonalAccountBalance(Profiles):

    def path(
        self, 
        stream_state: Mapping[str, Any] = None, 
        stream_slice: Mapping[str, Any] = None, 
        next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "borderless-accounts"


    def request_headers(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> Mapping[str, Any]:
        return {'Authorization': f'Bearer {self.api_token}'}

    
    def request_params(
            self,
            stream_state: Mapping[str, Any],
            stream_slice: Mapping[str, Any] = None,
            next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:
        # The api requires that we include the base currency as a query param so we do that in this method
        personalProfileId = Profiles(api_token=self.api_token, start_date=self.start_date).profilesID['personal_id']
        #print("CheckPersonalAccountBalance PERSONAL PROFILE ID", personalProfileId)
        return {'profileId': personalProfileId} # "id": 16178458, "type": "personal" |  "id": 16178459, "type": "business"
        # return {'currency': 'EUR', 'intervalStart': '2018-03-01T00:00:00.000Z', 'intervalEnd': '2018-03-15T23:59:59.999Z', 'type': 'COMPACT'}


    def parse_response(
        self, 
        response: requests.Response, 
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
        ) -> Iterable[Mapping]:
        """
        This method to define how a response is parsed.
        :return an iterable containing each record in the response
        """
        #profile = Profiles(api_token=self.api_token, start_date=self.start_date).parse_response(response)
        return response.json() # to parse the response from the API to match the schema of our schema .json file.


    def get_updated_state(
        self,
        current_stream_state: MutableMapping[str, Any],
        latest_record: Mapping[str, Any]
        ) -> Mapping[str, Any]:
        # This method is called once for each record returned from the API to compare the cursor field value in that record with the current state
        # we then return an updated state object. If this is the first time we run a sync or no state was passed, current_stream_state will be None.
        if current_stream_state is not None and 'date' in current_stream_state:
            current_parsed_date = datetime.strptime(current_stream_state['date'], '%Y-%m-%d')
            latest_record_date = datetime.strptime(latest_record['date'], '%Y-%m-%d')
            return {'date': max(current_parsed_date, latest_record_date).strftime('%Y-%m-%d')}
        else:
            return {'date': self.start_date.strftime('%Y-%m-%d')}
    

    def _chunk_date_range(self, start_date: datetime) -> List[Mapping[str, Any]]:
        """
        Returns a list of each day between the start date and now.
        The return value is a list of dicts {'date': date_string}.
        """
        dates = []
        while start_date < datetime.now():
            dates.append({'date': start_date.strftime('%Y-%m-%d')})
            start_date += timedelta(days=1)
        return dates

    
    def stream_slices(
        self,
        sync_mode,
        cursor_field: List[str] = None,
        stream_state: Mapping[str, Any] = None
        ) -> Iterable[Optional[Mapping[str, Any]]]:
        start_date = datetime.strptime(stream_state['date'], '%Y-%m-%d') if stream_state and 'date' in stream_state else self.start_date
        return self._chunk_date_range(start_date)


    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        """
        This method to define a pagination strategy. If you will not be using pagination, no action is required - just return None.

        This method should return a Mapping (e.g: dict) containing whatever information required to make paginated requests. This dict is passed
        to most other methods in this class to help you form headers, request bodies, query params, etc..

        For example, if the API accepts a 'page' parameter to determine which page of the result to return, and a response from the API contains a
        'page' number, then this method should probably return a dict {'page': response.json()['page'] + 1} to increment the page count by 1.
        The request_params method should then read the input next_page_token and set the 'page' param to next_page_token['page'].

        :param response: the most recent response from the API
        :return If there is another page in the result, a mapping (e.g: dict) containing information needed to query the next page in the response.
                If there are no more pages in the result, return None.
        """
        return None


# *********************************************************************************** 
# ***********************************CHECK BUSINESS ACCOUNT *************************
# ***********************************************************************************
class CheckBusinessAccountBalance(Profiles):
    
    def path(
        self, 
        stream_state: Mapping[str, Any] = None, 
        stream_slice: Mapping[str, Any] = None, 
        next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "borderless-accounts"


    def request_headers(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> Mapping[str, Any]:
        return {'Authorization': f'Bearer {self.api_token}'}

    
    def request_params(
            self,
            stream_state: Mapping[str, Any],
            stream_slice: Mapping[str, Any] = None,
            next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:
        # The api requires that we include the base currency as a query param so we do that in this method
        businessProfileId = Profiles(api_token=self.api_token, start_date=self.start_date).profilesID['business_id']
        #print("CheckBusinessAccountBalance BUSINESS PROFILE ID", businessProfileId)
        return {'profileId': businessProfileId} # "id": 16178458, "type": "personal" |  "id": 16178459, "type": "business"
        


    def parse_response(
        self, 
        response: requests.Response, 
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
        ) -> Iterable[Mapping]:
        """
        This method to define how a response is parsed.
        :return an iterable containing each record in the response
        """
        return response.json() # to parse the response from the API to match the schema of our schema .json file.


    def get_updated_state(
        self,
        current_stream_state: MutableMapping[str, Any],
        latest_record: Mapping[str, Any]
        ) -> Mapping[str, Any]:
        # This method is called once for each record returned from the API to compare the cursor field value in that record with the current state
        # we then return an updated state object. If this is the first time we run a sync or no state was passed, current_stream_state will be None.
        if current_stream_state is not None and 'date' in current_stream_state:
            current_parsed_date = datetime.strptime(current_stream_state['date'], '%Y-%m-%d')
            latest_record_date = datetime.strptime(latest_record['date'], '%Y-%m-%d')
            return {'date': max(current_parsed_date, latest_record_date).strftime('%Y-%m-%d')}
        else:
            return {'date': self.start_date.strftime('%Y-%m-%d')}
    

    def _chunk_date_range(self, start_date: datetime) -> List[Mapping[str, Any]]:
        """
        Returns a list of each day between the start date and now.
        The return value is a list of dicts {'date': date_string}.
        """
        dates = []
        while start_date < datetime.now():
            dates.append({'date': start_date.strftime('%Y-%m-%d')})
            start_date += timedelta(days=1)
        return dates

    
    def stream_slices(
        self,
        sync_mode,
        cursor_field: List[str] = None,
        stream_state: Mapping[str, Any] = None
        ) -> Iterable[Optional[Mapping[str, Any]]]:
        start_date = datetime.strptime(stream_state['date'], '%Y-%m-%d') if stream_state and 'date' in stream_state else self.start_date
        return self._chunk_date_range(start_date)


    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None


# *********************************************************************************** 
# *********************************** CHECK PERSONAL ACCOUNT ************************
# ***********************************************************************************
class SourceTranferwiseHttpApiExample(AbstractSource):
    
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        #headers = {'Authorization': f'Bearer {config["api_token"]}'}
        #print("api token: ", config["api_token"])
        if config["api_token"] is None:
            return False, f"Api token is invalid"
        else:
            return True, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        """
        TODO: Replace the streams below with your own streams.

        :param config: A Mapping of the user input configuration as defined in the connector spec.
        """
        # remove the authenticator if not required.
        auth = TokenAuthenticator(token=config['api_token']) # Oauth2Authenticator is also available if you need oauth support
        # auth = NoAuth()
        start_date = datetime.strptime(config["start_date"], "%Y-%m-%d")
        args = {'personal_profile_id':None, 'business_profile_id':None, 'authenticator': auth, 'api_token': config["api_token"], 'start_date': start_date}
        return [
            Profiles(authenticator=auth, api_token=config["api_token"], start_date=start_date),
            CheckPersonalAccountBalance(**args),
            CheckBusinessAccountBalance(**args)
          ]


# ************************ COMMANDS *********************************
# cd airbyte-projects/airbyte-test/airbyte-master/airbyte-integrations/connectors/source-tranferwise-http-api-example

# source .venv/Scripts/activate

# ROOT DIR: ./gradlew clean :airbyte-integrations:connectors:source-tranferwise-http-api-example:build

# ************************ PYTHON *********************************
# python main_dev.py spec
# python main_dev.py check --config secrets/config.json
# python main_dev.py check --config sample_files/config.json
# python main_dev.py discover --config secrets/config.json
# python main_dev.py read --config secrets/config.json --catalog sample_files/configured_catalog.json


# ****************************** DOCKER ***************************
# First build the container
# docker build . -t airbyte/source-tranferwise-http-api-example:dev-v0.1.0

# Then use the following commands to run it
# docker run --rm airbyte/source-tranferwise-http-api-example:dev-v0.1.0 spec
# docker run --rm -v $(pwd)/secrets:/secrets airbyte/source-tranferwise-http-api-example:dev-v0.1.0 check --config /secrets/config.json
# docker run --rm -v $(pwd)/secrets:/secrets airbyte/source-tranferwise-http-api-example:dev-v0.1.0 discover --config /secrets/config.json
# docker run --rm -v $(pwd)/secrets:/secrets -v $(pwd)/sample_files:/sample_files airbyte/source-tranferwise-http-api-example:dev-v0.1.0 read --config /secrets/config.json --catalog /sample_files/configured_catalog.json


