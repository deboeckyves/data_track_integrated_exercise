


# from geopy.geocoders import Nominatim
import os
import time

import requests

def get_country(lat, lon):
    url = f'https://nominatim.openstreetmap.org/reverse?lat={lat}&lon={lon}&format=json&accept-language=en'
    try:
        result = requests.get(url=url)
        result_json = result.json()
        print(result_json)
        return result_json['address']
    except:
        return None

print(get_country(51.2361942,4.385223684)) # results in Israel
print('41B011 - Berchem-Sainte-Agathe'.split()[2].strip())
# os.environ['TZ'] = 'Europe/London'
# time.tzset()
# print(time.strftime('%X %x %Z'))

# geolocator = Nominatim(user_agent="my_app")
# location = geolocator.reverse("50.624991569, 5.547464183", exactly_one=True)
# print(location.raw['address'])
# city = location.raw['address']['city']
# print(city)
