import pandas as pd
import country_converter as coco

def model(dbt, fal):
    ged = dbt.source('raw', 'ged')
    ged = pd.DataFrame({"country": ged.country.unique(), "iso3c": coco.convert(ged.country.unique(), to="ISO3")})
    #ged['iso3c'] = coco.convert(ged.country.unique(), to="ISO3")
    #ged['iso3c'] = coco.convert(ged.country, to="ISO3")
    return ged