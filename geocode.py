import time
import requests
import concurrent.futures
import pandas as pd

http = requests.Session()

# manages the request to the localhost/server running photon.jar


def geoRequest(loc):
    params = {'q': loc, 'limit': 30}
    url = f'http://localhost:2322/api'
    r = http.get(url, params=params,
                 headers={'user-agent': 'geocode-tester'})
    r = r.json()
    r.update({"location": loc})
    return r


def main():
    # read the searches.csv (contains all the queries we need)
    df = pd.read_csv("searches")
    # df to store the result
    addressDF = pd.DataFrame(columns=[
        'location',
        'osm_id',
        'osm_type',
        'name',
        'housenumber',
        'street',
        'postcode',
        'city',
        'state',
        'country',
        'osm_key',
        'osm_value',
        'lon',
        'lat',
        'message'
    ])
    with concurrent.futures.ThreadPoolExecutor() as executor:
        # we only want the "location"
        loc = df.loc[:, ['query']].to_numpy()

        # last 100 of the searches
        # on average: 100 takes around 7 seconds
        loc = loc[-100:]

        # use an executor map
        results = executor.map(geoRequest, loc)
        for result in results:
            for address in result['features']:
                g = address['geometry']['coordinates']
                p = address['properties']

                # get the correct columns for lon and lat
                g = {'lon': g[0], 'lat': g[1]}

                # remove excess columns
                p.pop('extent', None)
                p.pop('type', None)
                p.pop('district', None)
                p.pop('county', None)
                p.pop('countrycode', None)
                p.pop('locality', None)

                # join
                values = {**p, **g, 'location': result['location'][0]}
                addressToConcat = pd.DataFrame(data=values, index=[0])
                addressDF = pd.concat([addressDF, addressToConcat])
    # save as csv
    addressDF.to_csv('addresses.csv', index=False,
                     header=True, encoding='utf-8')


if __name__ == '__main__':
    start = time.perf_counter()
    main()
    finish = time.perf_counter()

    print(f'finished in {round(finish-start, 2)} seconds(s)')
