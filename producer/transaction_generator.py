import uuid, random
from datetime import datetime
from faker import Faker

fake = Faker('en_US')
CARD_IDS = [f'CARD_{str(i).zfill(4)}' for i in range(1, 21)]
MERCHANT_CATEGORIES = {
    'grocery':     {'merchants': ['Walmart','Kroger','Whole Foods'], 'min':10,  'max':200},
    'restaurant':  {'merchants': ['McDonalds','Chipotle','Starbucks'],'min':5,  'max':150},
    'electronics': {'merchants': ['Best Buy','Apple Store','Newegg'], 'min':50, 'max':3000},
    'travel':      {'merchants': ['Delta Airlines','Marriott','Airbnb'],'min':100,'max':5000},
    'online':      {'merchants': ['Amazon','eBay','Etsy'],             'min':5,  'max':1500},
}
CITIES = [
    {'city':'New York',   'lat':40.71, 'lon':-74.00, 'country':'US'},
    {'city':'Los Angeles','lat':34.05, 'lon':-118.24,'country':'US'},
    {'city':'Chicago',    'lat':41.87, 'lon':-87.62, 'country':'US'},
    {'city':'Houston',    'lat':29.76, 'lon':-95.36, 'country':'US'},
    {'city':'London',     'lat':51.50, 'lon':-0.12,  'country':'GB'},
    {'city':'Tokyo',      'lat':35.67, 'lon':139.65, 'country':'JP'},
]

def generate_transaction():
    category = random.choice(list(MERCHANT_CATEGORIES.keys()))
    cat      = MERCHANT_CATEGORIES[category]
    location = random.choice(CITIES)
    is_online = category == 'online' or random.random() < 0.3
    
    amount = round(random.uniform(cat['min'], cat['max']), 2)
    
    # Introduce random extreme outliers naturally into the stream
    if random.random() < 0.15:
        amount = round(random.uniform(5000, 25000), 2)
        
    return {
        'transaction_id':    str(uuid.uuid4()),
        'card_id':           random.choice(CARD_IDS),
        'timestamp':         datetime.utcnow().isoformat() + 'Z',
        'amount':            amount,
        'merchant':          random.choice(cat['merchants']),
        'merchant_category': category,
        'location_city':     location['city'],
        'location_country':  location['country'],
        'latitude':          location['lat'] + random.uniform(-0.1, 0.1),
        'longitude':         location['lon'] + random.uniform(-0.1, 0.1),
        'device':            'desktop' if is_online else random.choice(['POS','mobile']),
        'is_online':         is_online,
        'card_present':      not is_online
    }
